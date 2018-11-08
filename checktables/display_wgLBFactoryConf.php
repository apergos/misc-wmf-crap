<?php
# need to skip wfLoadSkin and wfLoadExtension
function wfLoadSkin( $skin, $path = null ) {
    return(0);
}
function wfLoadExtension( $ext, $path = null ) {
    return(0);
}

if ( $argc < 2 ) {
    fprintf( STDERR, "Usage: " . $argv[0] . " <path-to-dbcreds-file>\n" );
    exit(1);
}

# more crap we need to predefine
define( 'CACHE_NONE', 0 );
define( 'MEDIAWIKI', true );
$IP = strval(realpath( __DIR__ . '/..' ));
const NS_MAIN = 0;
define( 'NS_TALK', 1 );
const DBO_DEFAULT = 16;

include_once $argv[1];
$json_output = json_encode($wgLBFactoryConf);
fprintf( STDOUT, $json_output );
