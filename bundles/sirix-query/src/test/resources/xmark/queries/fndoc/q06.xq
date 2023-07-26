let $auction := doc("auction.xml") return
for $b in $auction//site/regions return count($b//item)
