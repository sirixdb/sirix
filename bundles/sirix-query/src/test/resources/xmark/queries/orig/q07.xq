xquery version "1.0";
let $auction := . return
for $p in $auction/site
return
  count($p//description) + count($p//annotation) + count($p//emailaddress)

