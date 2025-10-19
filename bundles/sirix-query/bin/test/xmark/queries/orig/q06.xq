xquery version "1.0";
let $auction := . return
for $b in $auction//site/regions return count($b//item)

