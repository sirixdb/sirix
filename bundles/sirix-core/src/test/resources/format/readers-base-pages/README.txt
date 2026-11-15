These 17 page images were serialized by unchanged 7cd676bcd engine classes,
captured before implementation changes and placed first on the runtime classpath.
Filenames are the decimal page-kind ID; gaps 7 and 14 are intentionally unassigned.
The normal resource builder defaults apply. The revision-root timestamp is zero.
Record page 1 holds raw bytes c8 0c 22 at slot 7, under directory sentinel 0.
This proves transport of a future full-byte kind above 63; kind 200 is unassigned
and still refuses if interpreted as a node by the current record serializer.
Overflow page 9 holds 01 02 03. HOT leaf 12 is an empty projection leaf.
HOT indirect 13 is a two-child single-mask span node. Other pages are empty roots.

Do not regenerate with the current writer: LegacyEnvelopeImagesTest depends on
these as an independent old-writer oracle for every page kind.
