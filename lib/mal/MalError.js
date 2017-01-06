/**
 * 
 */
class MalError {
  /**
   * errCode:
   * 0 - Status != 200, != 404
   * 1 - MAL's error message
   * 2 - Not found (404)
   * 3 - 200, but bad body
   * 4 - API (unofficial) validation error (for API server/client, not direct parser)
   */
  constructor(errMessage, statusCode, url = null, errCode = 0, errs = null) {
    this.url = url;
    this.statusCode = statusCode;
    this.errCode = errCode;
    this.errMessage = errMessage;
    this.errs = errs;
  }

  /**
   *
   */
  toString() {
    return "Error " + this.errCode + " (status " + this.statusCode + "): " + this.errMessage;
  }

  /**
   *
   */
  static fromJSON(obj) {
    let me = new cls();
    for (let k of ['url', 'statusCode', 'errCode', 'errMessage', 'errs']) {
      if (obj[k] !== undefined)
        me[k] = obj[k];
    }
    return me;
  }

}
var cls = MalError; //for using "cls.A" as like "self::A" inside class

module.exports = MalError;
