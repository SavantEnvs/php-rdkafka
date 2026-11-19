// Byte reader shared by the drivers: $data is the raw fuzz input.
$GLOBALS['p'] = 0;
function u8() { global $data, $p; return $p < strlen($data) ? ord($data[$p++]) : 0; }
function i64() { global $data, $p; $s = str_pad(substr($data, $p, 8), 8, "\0"); $p += 8; return unpack('q', $s)[1]; }
function i32() { global $data, $p; $s = str_pad(substr($data, $p, 4), 4, "\0"); $p += 4; return unpack('l', $s)[1]; }
function str() { global $data, $p; $l = u8(); $s = (string)substr($data, $p, $l); $p += $l; return $s; }
function more() { global $data, $p; return $p < strlen($data); }
