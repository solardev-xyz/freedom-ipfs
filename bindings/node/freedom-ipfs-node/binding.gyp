{
  "variables": {
    "freedom_ipfs_rust_repo%": "<(module_root_dir)/../../.."
  },
  "targets": [
    {
      "target_name": "freedom_ipfs_native",
      "sources": ["src/addon.cc"],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "<(freedom_ipfs_rust_repo)/ffi/include"
      ],
      "dependencies": ["<!(node -p \"require('node-addon-api').gyp\")"],
      "defines": ["NAPI_DISABLE_CPP_EXCEPTIONS"],
      "cflags_cc": ["-std=c++17"],
      "xcode_settings": {
        "CLANG_CXX_LANGUAGE_STANDARD": "c++17",
        "MACOSX_DEPLOYMENT_TARGET": "11.0"
      },
      "msvs_settings": {
        "VCCLCompilerTool": {
          "RuntimeLibrary": 2
        }
      },
      "conditions": [
        ["OS=='mac'", {
          "libraries": [
            "<(freedom_ipfs_rust_repo)/target/release/libfreedom_ipfs_mobile.a",
            "-framework Security",
            "-framework SystemConfiguration",
            "-framework CoreFoundation",
            "-framework CoreServices",
            "-framework IOKit",
            "-framework Foundation",
            "-liconv",
            "-lz"
          ]
        }],
        ["OS=='linux'", {
          "libraries": [
            "<(freedom_ipfs_rust_repo)/target/release/libfreedom_ipfs_mobile.a",
            "-ldl",
            "-lpthread",
            "-lm"
          ]
        }],
        ["OS=='win'", {
          "libraries": [
            "<(freedom_ipfs_rust_repo)/target/release/freedom_ipfs_mobile.lib",
            "ucrt.lib",
            "vcruntime.lib",
            "ws2_32.lib",
            "bcrypt.lib",
            "userenv.lib",
            "ntdll.lib",
            "advapi32.lib",
            "crypt32.lib",
            "secur32.lib",
            "ncrypt.lib"
          ]
        }]
      ]
    }
  ]
}
