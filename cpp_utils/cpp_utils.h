#ifndef CPP_UTILS_H
#define CPP_UTILS_H

#include <node.h>

#define GET_CONTENTS(view) \
(static_cast<unsigned char*>(view->Buffer()->GetContents().Data()) + view->ByteOffset())

void dAlsBuildSubFixedFacts(const v8::FunctionCallbackInfo<v8::Value>& info);
void sAlsBuildSubFixedFacts(const v8::FunctionCallbackInfo<v8::Value>& info);

#endif //CPP_UTILS_H
