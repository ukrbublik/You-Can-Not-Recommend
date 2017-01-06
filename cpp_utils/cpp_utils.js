(function () {
  'use strict';

  var cpp_utils = require('../build/Release/cpp_utils');

  function typeCheck(array) {
    if (array.constructor === Float64Array)
      return true;
    else if (array.constructor === Float32Array)
      return false;

    throw new Error('invalid type!');
  }

  cpp_utils.alsBuildSubFixedFacts = function(subFixedFacts, fixedFacts, indx, cols, factorsCount) {
    return typeCheck(subFixedFacts) ?
      cpp_utils.dAlsBuildSubFixedFacts(subFixedFacts, fixedFacts, indx, cols, factorsCount) :
      cpp_utils.sAlsBuildSubFixedFacts(subFixedFacts, fixedFacts, indx, cols, factorsCount);
  };

  module.exports = cpp_utils;
}());
