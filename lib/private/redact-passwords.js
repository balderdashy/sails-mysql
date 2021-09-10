//  ██████╗ ███████╗██████╗  █████╗  ██████╗████████╗
//  ██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔════╝╚══██╔══╝
//  ██████╔╝█████╗  ██║  ██║███████║██║        ██║
//  ██╔══██╗██╔══╝  ██║  ██║██╔══██║██║        ██║
//  ██║  ██║███████╗██████╔╝██║  ██║╚██████╗   ██║
//  ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═╝  ╚═╝ ╚═════╝   ╚═╝
//
//  ██████╗  █████╗ ███████╗███████╗██╗    ██╗ ██████╗ ██████╗ ██████╗ ███████╗
//  ██╔══██╗██╔══██╗██╔════╝██╔════╝██║    ██║██╔═══██╗██╔══██╗██╔══██╗██╔════╝
//  ██████╔╝███████║███████╗███████╗██║ █╗ ██║██║   ██║██████╔╝██║  ██║███████╗
//  ██╔═══╝ ██╔══██║╚════██║╚════██║██║███╗██║██║   ██║██╔══██╗██║  ██║╚════██║
//  ██║     ██║  ██║███████║███████║╚███╔███╔╝╚██████╔╝██║  ██║██████╔╝███████║
//  ╚═╝     ╚═╝  ╚═╝╚══════╝╚══════╝ ╚══╝╚══╝  ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚══════╝
//
//  Remove database passwords from the error instance.

module.exports = function redactPasswords(err) {
  var REDACT_REPLACEMENT = '$1:****@';
  var REDACT_REGEX_SINGLE = /^(mysql:\/\/[^:\s]*):[^@\s]*@/;
  var REDACT_REGEX_MULTI =   /(mysql:\/\/[^:\s]*):[^@\s]*@/g;

  if(err) {
    if(err.meta && typeof err.meta === 'object' && err.meta.password && typeof err.meta.password === 'string'){
      err.meta.password = '****';
    }
    if(err.meta && typeof err.meta === 'object' && err.meta.url && typeof err.meta.url === 'string') {
      err.meta.url = err.meta.url.replace(REDACT_REGEX_SINGLE, REDACT_REPLACEMENT);
    }
    if(err.message && typeof err.message === 'string') {
      err.message = err.message.replace(REDACT_REGEX_MULTI, REDACT_REPLACEMENT);
    }
  }
  return err;
};
