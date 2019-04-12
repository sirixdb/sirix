let $statuses := jn:doc('mycol.jn','mydoc.jn')=>statuses
let $i := 0
let $createdGreaterThanMai := for $i in (0 to bit:len($statuses) - 1)
  let $status := $statuses[[$i]]
  let $dateTimeCreated := xs:dateTime($status=>created_in)
  where $dateTimeCreated > xs:dateTime("2018-05-03T13:20:00")
  order by $dateTimeCreated
  return $dateTimeCreated
return $createdGreaterThanMai
