---
kind: row
---
| An HTTP route authorized one role for a method that both creates and replaces; the handler learned it was replacing only after the check and destroyed history under CREATE. | When one entry point has several effects, authorize at the point where the effect is known, before acting, and specify the rule once for every entry point (REST and query). Test the decision offline with each missing role. | [REST PUT replace write-up](postmortems/rest-put-replace-authorization.md), `CreateOrReplaceResourceAuthorizationTest` |
