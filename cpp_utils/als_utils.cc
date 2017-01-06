#include "cpp_utils.h"
#include <string.h>

void dAlsBuildSubFixedFacts(const v8::FunctionCallbackInfo<v8::Value>& info) {
	double *subFixedFacts = reinterpret_cast<double*>(GET_CONTENTS(info[0].As<v8::Float64Array>()));
	double *fixedFacts = reinterpret_cast<double*>(GET_CONTENTS(info[1].As<v8::Float64Array>()));
	int *indx = reinterpret_cast<int*>(GET_CONTENTS(info[2].As<v8::Int32Array>()));
	int cols = info[3]->Int32Value();
	int factorsCount = info[4]->Int32Value();

	int c, colId;
	for (c = 0 ; c < cols ; c++) {
		colId = indx[c];
		memcpy(
			(void*) (subFixedFacts + c * factorsCount), 
			(void*) (fixedFacts + colId * factorsCount), 
			1 * factorsCount * sizeof(double)
		);
	}
}

void sAlsBuildSubFixedFacts(const v8::FunctionCallbackInfo<v8::Value>& info) {
	float *subFixedFacts = reinterpret_cast<float*>(GET_CONTENTS(info[0].As<v8::Float32Array>()));
	float *fixedFacts = reinterpret_cast<float*>(GET_CONTENTS(info[1].As<v8::Float32Array>()));
	int *indx = reinterpret_cast<int*>(GET_CONTENTS(info[2].As<v8::Int32Array>()));
	int cols = info[3]->Int32Value();
	int factorsCount = info[4]->Int32Value();

	int c, colId;
	for (c = 0 ; c < cols ; c++) {
		colId = indx[c];
		memcpy(
			(void*) (subFixedFacts + c * factorsCount), 
			(void*) (fixedFacts + colId * factorsCount), 
			1 * factorsCount * sizeof(float)
		);
	}
}
