#include "cpp_utils.h"

void Init(v8::Local<v8::Object> exports) {
  NODE_SET_METHOD(exports, "dAlsBuildSubFixedFacts", dAlsBuildSubFixedFacts);
  NODE_SET_METHOD(exports, "sAlsBuildSubFixedFacts", sAlsBuildSubFixedFacts);
}

NODE_MODULE(cpp_utils, Init)
