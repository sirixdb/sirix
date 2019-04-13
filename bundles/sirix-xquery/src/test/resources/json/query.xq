let $statuses := jn:open('mycol.jn','mydoc.jn', xs:dateTime('$$$$'))=>statuses
let $foundStatus := for $status in bit:array-values($statuses)
  let $dateTimeCreated := xs:dateTime($status=>created_at)
  where $dateTimeCreated > xs:dateTime("2018-02-01T00:00:00")
  order by $dateTimeCreated
  return $dateTimeCreated
return $foundStatus
