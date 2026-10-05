#include <napi.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>

extern "C" {
#include "freedom_ipfs.h"
}

namespace {

constexpr uint64_t kDefaultMaxCacheBytes = 256ull * 1024ull * 1024ull;

Napi::Value ThrowTypeError(Napi::Env env, const char* message) {
  Napi::TypeError::New(env, message).ThrowAsJavaScriptException();
  return env.Null();
}

uint64_t Uint64FromValue(const Napi::Value& value, bool* ok) {
  if (value.IsString()) {
    const std::string text = value.As<Napi::String>().Utf8Value();
    if (text.empty()) {
      *ok = false;
      return 0;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long out = std::strtoull(text.c_str(), &end, 10);
    *ok = errno == 0 && end != nullptr && *end == '\0';
    return *ok ? static_cast<uint64_t>(out) : 0;
  }
  if (value.IsNumber()) {
    const double n = value.As<Napi::Number>().DoubleValue();
    if (n >= 0 && n <= static_cast<double>(std::numeric_limits<uint64_t>::max())) {
      *ok = true;
      return static_cast<uint64_t>(n);
    }
  }
  *ok = false;
  return 0;
}

FreedomIpfsNode* NodeFromValue(const Napi::Value& value, bool* ok) {
  const uint64_t raw = Uint64FromValue(value, ok);
  if (!*ok || raw == 0) {
    *ok = false;
    return nullptr;
  }
  return reinterpret_cast<FreedomIpfsNode*>(raw);
}

Napi::String StringFromU64(Napi::Env env, uint64_t value) {
  return Napi::String::New(env, std::to_string(value));
}

Napi::String StringFromNode(Napi::Env env, FreedomIpfsNode* node) {
  return StringFromU64(env, reinterpret_cast<uint64_t>(node));
}

std::string TakeCString(char* ptr) {
  if (ptr == nullptr) {
    return "";
  }
  std::string out(ptr);
  freedom_ipfs_string_free(ptr);
  return out;
}

// Async lifecycle calls (start/stop/free) on one node handle run one at a
// time, in the order JS made them: each waits here until the previous one has
// settled, so an unawaited start cannot finish after a later stop, and free
// runs after every start/stop queued before it. Per-env instance data, only
// touched on that env's JS thread.
struct LifecycleQueues {
  std::unordered_map<FreedomIpfsNode*, std::deque<Napi::AsyncWorker*>> pending;
};

LifecycleQueues& Lifecycle(Napi::Env env) { return *env.GetInstanceData<LifecycleQueues>(); }

bool LifecyclePending(Napi::Env env, FreedomIpfsNode* node) {
  return Lifecycle(env).pending.count(node) != 0;
}

// The sync lifecycle exports refuse to run while an async lifecycle call on
// the same handle is still pending (they would race it, or free the node
// underneath it).
bool ThrowIfLifecyclePending(Napi::Env env, FreedomIpfsNode* node, const char* name) {
  if (!LifecyclePending(env, node)) return false;
  Napi::Error::New(env, std::string(name) +
                            ": an async lifecycle call on this node is still pending")
      .ThrowAsJavaScriptException();
  return true;
}

// Runs `Work` on the libuv thread pool and settles a Promise on the JS thread
// with `Resolve(env, result)`. The lifecycle calls below can open SQLite,
// build or tear down a Tokio runtime, and wait for in-flight requests to
// unwind, so the `*Async` exports use this to keep Electron's main process
// responsive. The sync exports stay for hosts that already rely on them.
// A non-null `serial` node puts the worker in that node's lifecycle queue.
template <typename Result, typename Work, typename Resolve>
class PromiseWorker : public Napi::AsyncWorker {
 public:
  PromiseWorker(Napi::Env env, const char* resource_name, FreedomIpfsNode* serial, Work work,
                Resolve resolve)
      : Napi::AsyncWorker(env, resource_name),
        deferred_(Napi::Promise::Deferred::New(env)),
        serial_(serial),
        work_(std::move(work)),
        resolve_(std::move(resolve)) {}

  Napi::Promise Promise() const { return deferred_.Promise(); }

  void Execute() override { result_ = work_(); }

  void OnOK() override {
    RunNext();
    deferred_.Resolve(resolve_(Env(), result_));
  }

  void OnError(const Napi::Error& error) override {
    RunNext();
    deferred_.Reject(error.Value());
  }

 private:
  // This worker is at the front of its node's queue; hand over to the next.
  void RunNext() {
    if (serial_ == nullptr) return;
    auto& pending = Lifecycle(Env()).pending;
    auto it = pending.find(serial_);
    if (it == pending.end()) return;
    it->second.pop_front();
    if (it->second.empty()) {
      pending.erase(it);
    } else {
      it->second.front()->Queue();
    }
  }

  Napi::Promise::Deferred deferred_;
  FreedomIpfsNode* serial_;
  Work work_;
  Resolve resolve_;
  Result result_{};
};

template <typename Result, typename Work, typename Resolve>
Napi::Value QueuePromise(Napi::Env env, const char* resource_name, FreedomIpfsNode* serial,
                         Work work, Resolve resolve) {
  auto* worker = new PromiseWorker<Result, Work, Resolve>(
      env, resource_name, serial, std::move(work), std::move(resolve));
  Napi::Promise promise = worker->Promise();
  if (serial == nullptr) {
    worker->Queue();
    return promise;
  }
  auto& queue = Lifecycle(env).pending[serial];
  queue.push_back(worker);
  if (queue.size() == 1) worker->Queue();
  return promise;
}

Napi::Value Version(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), TakeCString(freedom_ipfs_version()));
}

Napi::Value BuildInfoJson(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), TakeCString(freedom_ipfs_build_info_json()));
}

struct NewArgs {
  std::string data_dir;
  uint64_t max_cache_bytes = kDefaultMaxCacheBytes;
};

bool ParseNewArgs(const Napi::CallbackInfo& info, const char* name, NewArgs* out) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    ThrowTypeError(env, (std::string(name) + "(dataDir, maxCacheBytes) requires a dataDir string").c_str());
    return false;
  }
  out->data_dir = info[0].As<Napi::String>().Utf8Value();
  if (info.Length() > 1 && !info[1].IsUndefined() && !info[1].IsNull()) {
    bool ok = false;
    out->max_cache_bytes = Uint64FromValue(info[1], &ok);
    if (!ok) {
      ThrowTypeError(env, "maxCacheBytes must be a non-negative integer");
      return false;
    }
  }
  return true;
}

Napi::Value NodeNewWithDataDir(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  NewArgs args;
  if (!ParseNewArgs(info, "nodeNewWithDataDir", &args)) return env.Null();
  FreedomIpfsNode* node =
      freedom_ipfs_node_new_with_data_dir(args.data_dir.c_str(), args.max_cache_bytes);
  return StringFromNode(env, node);
}

// Resolves to the node handle string, or "0" when the node could not be
// created (same contract as nodeNewWithDataDir).
Napi::Value NodeNewWithDataDirAsync(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  NewArgs args;
  if (!ParseNewArgs(info, "nodeNewWithDataDirAsync", &args)) return env.Null();
  return QueuePromise<FreedomIpfsNode*>(
      env, "freedomIpfsNodeNew", nullptr,
      [args]() {
        return freedom_ipfs_node_new_with_data_dir(args.data_dir.c_str(), args.max_cache_bytes);
      },
      [](Napi::Env env, FreedomIpfsNode* node) -> Napi::Value {
        return StringFromNode(env, node);
      });
}

Napi::Value NodeFree(const Napi::CallbackInfo& info) {
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (ok) {
    if (ThrowIfLifecyclePending(info.Env(), node, "nodeFree")) return info.Env().Null();
    freedom_ipfs_node_free(node);
  }
  return info.Env().Undefined();
}

// The handle is invalid as soon as this is called: the caller must not pass
// it to any other export. The free itself runs after any async start/stop on
// the same handle that was called before it.
Napi::Value NodeFreeAsync(const Napi::CallbackInfo& info) {
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  return QueuePromise<bool>(
      info.Env(), "freedomIpfsNodeFree", ok ? node : nullptr,
      [node, ok]() {
        if (ok) freedom_ipfs_node_free(node);
        return ok;
      },
      [](Napi::Env env, bool) -> Napi::Value { return env.Undefined(); });
}

struct StartArgs {
  FreedomIpfsNode* node = nullptr;
  bool has_delegated_router = false;
  std::string delegated_router;
  uint32_t routing_mode = FREEDOM_IPFS_ROUTING_MODE_AUTO;
  size_t max_concurrent_requests = 0;
  uint64_t dht_query_timeout_secs = 0;
  size_t dht_max_providers = 0;
  uint64_t request_queue_timeout_ms = 0;

  bool Start() const {
    return freedom_ipfs_node_start_native_gateway_online_with_config_v3(
        node, has_delegated_router ? delegated_router.c_str() : nullptr, routing_mode,
        max_concurrent_requests, dht_query_timeout_secs, dht_max_providers,
        request_queue_timeout_ms);
  }
};

bool ParseStartArgs(const Napi::CallbackInfo& info, StartArgs* out) {
  Napi::Env env = info.Env();
  bool ok = false;
  out->node = NodeFromValue(info[0], &ok);
  if (!ok) {
    ThrowTypeError(env, "invalid node handle");
    return false;
  }
  if (info.Length() > 1 && info[1].IsString()) {
    out->delegated_router = info[1].As<Napi::String>().Utf8Value();
    out->has_delegated_router = !out->delegated_router.empty();
  }
  if (info.Length() > 2 && info[2].IsNumber()) {
    out->routing_mode = info[2].As<Napi::Number>().Uint32Value();
  }
  if (info.Length() > 3 && info[3].IsNumber()) {
    out->max_concurrent_requests =
        static_cast<size_t>(info[3].As<Napi::Number>().Uint32Value());
  }
  if (info.Length() > 4 && !info[4].IsUndefined() && !info[4].IsNull()) {
    out->dht_query_timeout_secs = Uint64FromValue(info[4], &ok);
    if (!ok) {
      ThrowTypeError(env, "dhtQueryTimeoutSecs must be an integer");
      return false;
    }
  }
  if (info.Length() > 5 && info[5].IsNumber()) {
    out->dht_max_providers = static_cast<size_t>(info[5].As<Napi::Number>().Uint32Value());
  }
  if (info.Length() > 6 && !info[6].IsUndefined() && !info[6].IsNull()) {
    out->request_queue_timeout_ms = Uint64FromValue(info[6], &ok);
    if (!ok) {
      ThrowTypeError(env, "requestQueueTimeoutMs must be an integer");
      return false;
    }
  }
  return true;
}

Napi::Value NodeStartNativeGatewayOnline(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  StartArgs args;
  if (!ParseStartArgs(info, &args)) return env.Null();
  if (ThrowIfLifecyclePending(env, args.node, "nodeStartNativeGatewayOnline")) return env.Null();
  return Napi::Boolean::New(env, args.Start());
}

// Same arguments as nodeStartNativeGatewayOnline; resolves to its boolean.
Napi::Value NodeStartNativeGatewayOnlineAsync(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  StartArgs args;
  if (!ParseStartArgs(info, &args)) return env.Null();
  return QueuePromise<bool>(
      env, "freedomIpfsNodeStart", args.node, [args]() { return args.Start(); },
      [](Napi::Env env, bool started) -> Napi::Value {
        return Napi::Boolean::New(env, started);
      });
}

Napi::Value NodeStopGateway(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  if (ThrowIfLifecyclePending(env, node, "nodeStopGateway")) return env.Null();
  return Napi::Boolean::New(env, freedom_ipfs_node_stop_gateway(node));
}

Napi::Value NodeStopGatewayAsync(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  return QueuePromise<bool>(
      env, "freedomIpfsNodeStopGateway", node,
      [node]() { return freedom_ipfs_node_stop_gateway(node); },
      [](Napi::Env env, bool stopped) -> Napi::Value {
        return Napi::Boolean::New(env, stopped);
      });
}

Napi::Value StringJsonCall(const Napi::CallbackInfo& info, char* (*fn)(FreedomIpfsNode*)) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  return Napi::String::New(env, TakeCString(fn(node)));
}

Napi::Value NodeProgressSnapshotJson(const Napi::CallbackInfo& info) {
  return StringJsonCall(info, freedom_ipfs_node_progress_snapshot_json);
}

Napi::Value NodeNativeGatewayStatsJson(const Napi::CallbackInfo& info) {
  return StringJsonCall(info, freedom_ipfs_node_native_gateway_stats_json);
}

Napi::Value NodeClearProgress(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  return Napi::Boolean::New(env, freedom_ipfs_node_clear_progress(node));
}

Napi::Value NodeClearCache(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  return Napi::Boolean::New(env, freedom_ipfs_node_clear_cache(node));
}

Napi::Value GatewayRequestStart(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  if (info.Length() < 2 || !info[1].IsString()) {
    return ThrowTypeError(env, "gatewayRequestStart(node, requestJson) requires requestJson");
  }
  const std::string request_json = info[1].As<Napi::String>().Utf8Value();
  return StringFromU64(env, freedom_ipfs_gateway_request_start(node, request_json.c_str()));
}

Napi::Value GatewayRequestResponseJson(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  const uint64_t handle = Uint64FromValue(info[1], &ok);
  if (!ok) return ThrowTypeError(env, "invalid request handle");
  return Napi::String::New(
      env, TakeCString(freedom_ipfs_gateway_request_response_json(node, handle)));
}

Napi::Value GatewayRequestRead(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  const uint64_t handle = Uint64FromValue(info[1], &ok);
  if (!ok) return ThrowTypeError(env, "invalid request handle");
  if (info.Length() < 3 || !info[2].IsBuffer()) {
    return ThrowTypeError(env, "gatewayRequestRead(node, handle, buffer) requires a Buffer");
  }
  Napi::Buffer<uint8_t> buffer = info[2].As<Napi::Buffer<uint8_t>>();
  FreedomIpfsGatewayReadResult result =
      freedom_ipfs_gateway_request_read(node, handle, buffer.Data(), buffer.Length());
  Napi::Object out = Napi::Object::New(env);
  out.Set("status", Napi::Number::New(env, result.status));
  out.Set("bytesRead", Napi::Number::New(env, static_cast<double>(result.bytes_read)));
  return out;
}

Napi::Value GatewayWaitNextEvent(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  uint64_t timeout_ms = 0;
  if (info.Length() > 1 && !info[1].IsUndefined() && !info[1].IsNull()) {
    timeout_ms = Uint64FromValue(info[1], &ok);
    if (!ok) return ThrowTypeError(env, "timeoutMs must be an integer");
  }
  FreedomIpfsGatewayEvent event = freedom_ipfs_gateway_wait_next_event(node, timeout_ms);
  Napi::Object out = Napi::Object::New(env);
  out.Set("status", Napi::Number::New(env, event.status));
  out.Set("events", Napi::Number::New(env, event.events));
  out.Set("requestHandle", StringFromU64(env, event.request_handle));
  return out;
}

Napi::Value GatewayRequestCancel(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  const uint64_t handle = Uint64FromValue(info[1], &ok);
  if (!ok) return ThrowTypeError(env, "invalid request handle");
  return Napi::Boolean::New(env, freedom_ipfs_gateway_request_cancel(node, handle));
}

Napi::Value GatewayRequestFree(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  const uint64_t handle = Uint64FromValue(info[1], &ok);
  if (!ok) return ThrowTypeError(env, "invalid request handle");
  return Napi::Boolean::New(env, freedom_ipfs_gateway_request_free(node, handle));
}

Napi::Object Constants(Napi::Env env) {
  Napi::Object out = Napi::Object::New(env);
  out.Set("MOBILE_FFI_ABI_VERSION", FREEDOM_IPFS_MOBILE_FFI_ABI_VERSION);
  out.Set("READ_PENDING", FREEDOM_IPFS_GATEWAY_READ_PENDING);
  out.Set("READ_BYTES", FREEDOM_IPFS_GATEWAY_READ_BYTES);
  out.Set("READ_END", FREEDOM_IPFS_GATEWAY_READ_END);
  out.Set("READ_CANCELLED", FREEDOM_IPFS_GATEWAY_READ_CANCELLED);
  out.Set("READ_FAILED", FREEDOM_IPFS_GATEWAY_READ_FAILED);
  out.Set("READ_INVALID_HANDLE", FREEDOM_IPFS_GATEWAY_READ_INVALID_HANDLE);
  out.Set("EVENT_STATUS_OK", FREEDOM_IPFS_GATEWAY_EVENT_STATUS_OK);
  out.Set("EVENT_STATUS_TIMEOUT", FREEDOM_IPFS_GATEWAY_EVENT_STATUS_TIMEOUT);
  out.Set("EVENT_STATUS_INVALID_NODE", FREEDOM_IPFS_GATEWAY_EVENT_STATUS_INVALID_NODE);
  out.Set("EVENT_STATUS_GATEWAY_STOPPED", FREEDOM_IPFS_GATEWAY_EVENT_STATUS_GATEWAY_STOPPED);
  out.Set("EVENT_RESPONSE_READY", FREEDOM_IPFS_GATEWAY_EVENT_RESPONSE_READY);
  out.Set("EVENT_BODY_READY", FREEDOM_IPFS_GATEWAY_EVENT_BODY_READY);
  out.Set("EVENT_END", FREEDOM_IPFS_GATEWAY_EVENT_END);
  out.Set("EVENT_FAILED", FREEDOM_IPFS_GATEWAY_EVENT_FAILED);
  out.Set("EVENT_CANCELLED", FREEDOM_IPFS_GATEWAY_EVENT_CANCELLED);
  out.Set("EVENT_HANDLE_FREED", FREEDOM_IPFS_GATEWAY_EVENT_HANDLE_FREED);
  out.Set("ROUTING_MODE_AUTO", FREEDOM_IPFS_ROUTING_MODE_AUTO);
  out.Set("ROUTING_MODE_DELEGATED", FREEDOM_IPFS_ROUTING_MODE_DELEGATED);
  out.Set("ROUTING_MODE_LIGHT_DHT", FREEDOM_IPFS_ROUTING_MODE_LIGHT_DHT);
  out.Set("ROUTING_MODE_OFFLINE", FREEDOM_IPFS_ROUTING_MODE_OFFLINE);
  return out;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
  env.SetInstanceData(new LifecycleQueues());
  exports.Set("version", Napi::Function::New(env, Version));
  exports.Set("buildInfoJson", Napi::Function::New(env, BuildInfoJson));
  exports.Set("nodeNewWithDataDir", Napi::Function::New(env, NodeNewWithDataDir));
  exports.Set("nodeNewWithDataDirAsync", Napi::Function::New(env, NodeNewWithDataDirAsync));
  exports.Set("nodeFree", Napi::Function::New(env, NodeFree));
  exports.Set("nodeFreeAsync", Napi::Function::New(env, NodeFreeAsync));
  exports.Set(
      "nodeStartNativeGatewayOnline",
      Napi::Function::New(env, NodeStartNativeGatewayOnline));
  exports.Set(
      "nodeStartNativeGatewayOnlineAsync",
      Napi::Function::New(env, NodeStartNativeGatewayOnlineAsync));
  exports.Set("nodeStopGateway", Napi::Function::New(env, NodeStopGateway));
  exports.Set("nodeStopGatewayAsync", Napi::Function::New(env, NodeStopGatewayAsync));
  exports.Set("nodeProgressSnapshotJson", Napi::Function::New(env, NodeProgressSnapshotJson));
  exports.Set("nodeNativeGatewayStatsJson", Napi::Function::New(env, NodeNativeGatewayStatsJson));
  exports.Set("nodeClearProgress", Napi::Function::New(env, NodeClearProgress));
  exports.Set("nodeClearCache", Napi::Function::New(env, NodeClearCache));
  exports.Set("gatewayRequestStart", Napi::Function::New(env, GatewayRequestStart));
  exports.Set("gatewayRequestResponseJson", Napi::Function::New(env, GatewayRequestResponseJson));
  exports.Set("gatewayRequestRead", Napi::Function::New(env, GatewayRequestRead));
  exports.Set("gatewayWaitNextEvent", Napi::Function::New(env, GatewayWaitNextEvent));
  exports.Set("gatewayRequestCancel", Napi::Function::New(env, GatewayRequestCancel));
  exports.Set("gatewayRequestFree", Napi::Function::New(env, GatewayRequestFree));
  exports.Set("constants", Constants(env));
  return exports;
}

}  // namespace

NODE_API_MODULE(freedom_ipfs_native, Init)
