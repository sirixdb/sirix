declare namespace local = "http://www.foobar.org";
declare function local:convert($v as xs:decimal?) as xs:decimal?
{
  2.20371 * $v (: convert Dfl to Euro :)
};

let $auction := $$ return
for $i in $auction/site/open_auctions/open_auction
return local:convert(zero-or-one($i/reserve))
