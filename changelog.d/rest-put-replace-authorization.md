---
section: Fixed
---
- **REST PUT needs MODIFY and DELETE to replace an existing resource** — `PUT /:database/:resource`
  and the multipart `PUT /:database` replaced an existing resource, discarding its history, with
  only the CREATE role. Replacing now also needs MODIFY and DELETE, checked before anything is
  removed, and a refused replacement answers 403 with the resource unchanged. Creating a new
  resource still needs only CREATE (REST-210)
  ([write-up](docs/postmortems/rest-put-replace-authorization.md), #1477).
