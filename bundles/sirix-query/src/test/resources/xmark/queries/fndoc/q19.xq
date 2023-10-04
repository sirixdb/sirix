let $auction := doc("auction.xml") return
for $b in $auction/site/regions//item
let $k := $b/name/text()
order by zero-or-one($b/location) ascending 
return <item name="{$k}">{$b/location/text()}</item>
