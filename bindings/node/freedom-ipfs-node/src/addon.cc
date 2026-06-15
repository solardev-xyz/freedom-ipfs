#include <napi.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

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

Napi::Value Version(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), TakeCString(freedom_ipfs_version()));
}

Napi::Value BuildInfoJson(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), TakeCString(freedom_ipfs_build_info_json()));
}

Napi::Value NodeNewWithDataDir(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1 || !info[0].IsString()) {
    return ThrowTypeError(env, "nodeNewWithDataDir(dataDir, maxCacheBytes) requires a dataDir string");
  }
  const std::string data_dir = info[0].As<Napi::String>().Utf8Value();
  uint64_t max_cache_bytes = kDefaultMaxCacheBytes;
  if (info.Length() > 1 && !info[1].IsUndefined() && !info[1].IsNull()) {
    bool ok = false;
    max_cache_bytes = Uint64FromValue(info[1], &ok);
    if (!ok) {
      return ThrowTypeError(env, "maxCacheBytes must be a non-negative integer");
    }
  }
  FreedomIpfsNode* node =
      freedom_ipfs_node_new_with_data_dir(data_dir.c_str(), max_cache_bytes);
  return StringFromNode(env, node);
}

Napi::Value NodeFree(const Napi::CallbackInfo& info) {
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (ok) {
    freedom_ipfs_node_free(node);
  }
  return info.Env().Undefined();
}

Napi::Value NodeStartNativeGatewayOnline(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");

  std::string delegated_router;
  const char* delegated_router_ptr = nullptr;
  if (info.Length() > 1 && info[1].IsString()) {
    delegated_router = info[1].As<Napi::String>().Utf8Value();
    if (!delegated_router.empty()) delegated_router_ptr = delegated_router.c_str();
  }
  uint32_t routing_mode = FREEDOM_IPFS_ROUTING_MODE_AUTO;
  if (info.Length() > 2 && info[2].IsNumber()) {
    routing_mode = info[2].As<Napi::Number>().Uint32Value();
  }
  size_t max_concurrent_requests = 0;
  if (info.Length() > 3 && info[3].IsNumber()) {
    max_concurrent_requests =
        static_cast<size_t>(info[3].As<Napi::Number>().Uint32Value());
  }
  uint64_t dht_query_timeout_secs = 0;
  if (info.Length() > 4 && !info[4].IsUndefined() && !info[4].IsNull()) {
    dht_query_timeout_secs = Uint64FromValue(info[4], &ok);
    if (!ok) return ThrowTypeError(env, "dhtQueryTimeoutSecs must be an integer");
  }
  size_t dht_max_providers = 0;
  if (info.Length() > 5 && info[5].IsNumber()) {
    dht_max_providers = static_cast<size_t>(info[5].As<Napi::Number>().Uint32Value());
  }
  uint64_t request_queue_timeout_ms = 0;
  if (info.Length() > 6 && !info[6].IsUndefined() && !info[6].IsNull()) {
    request_queue_timeout_ms = Uint64FromValue(info[6], &ok);
    if (!ok) return ThrowTypeError(env, "requestQueueTimeoutMs must be an integer");
  }

  const bool started = freedom_ipfs_node_start_native_gateway_online_with_config_v3(
      node, delegated_router_ptr, routing_mode, max_concurrent_requests,
      dht_query_timeout_secs, dht_max_providers, request_queue_timeout_ms);
  return Napi::Boolean::New(env, started);
}

Napi::Value NodeStopGateway(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  bool ok = false;
  FreedomIpfsNode* node = NodeFromValue(info[0], &ok);
  if (!ok) return ThrowTypeError(env, "invalid node handle");
  return Napi::Boolean::New(env, freedom_ipfs_node_stop_gateway(node));
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
  exports.Set("version", Napi::Function::New(env, Version));
  exports.Set("buildInfoJson", Napi::Function::New(env, BuildInfoJson));
  exports.Set("nodeNewWithDataDir", Napi::Function::New(env, NodeNewWithDataDir));
  exports.Set("nodeFree", Napi::Function::New(env, NodeFree));
  exports.Set(
      "nodeStartNativeGatewayOnline",
      Napi::Function::New(env, NodeStartNativeGatewayOnline));
  exports.Set("nodeStopGateway", Napi::Function::New(env, NodeStopGateway));
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
