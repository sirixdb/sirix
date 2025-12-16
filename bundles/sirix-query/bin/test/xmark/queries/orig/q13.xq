xquery version "1.0";
let $auction := . return
for $i in $auction/site/regions/australia/item
return <item name="{$i/name/text()}">{$i/description}</item>

