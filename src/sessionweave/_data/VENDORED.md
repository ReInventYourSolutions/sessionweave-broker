# Vendored third-party assets

These files are bundled so the broker can serve its docs hub fully offline
(air-gapped deployments). They are third-party works under their own licenses,
**not** covered by this repository's Apache-2.0 license (see the root `LICENSE`).

| Path | Project | Version | License |
|------|---------|---------|---------|
| `swagger-ui/swagger-ui-bundle.js`, `swagger-ui/swagger-ui.css` | [Swagger UI](https://github.com/swagger-api/swagger-ui) | 5.3.2 (`swagger-ui-dist@5`) | Apache-2.0 |
| `vendor/marked.min.js` | [marked](https://github.com/markedjs/marked) | 12.0.2 | MIT |

License texts: Swagger UI's bundled-dependency notices are in
`swagger-ui/swagger-ui-bundle.js.LICENSE.txt`; marked's MIT header is inline at
the top of `vendor/marked.min.js`.

`skill.md` is this repo's own participant skill (`skills/sessionweave-participant/SKILL.md`),
kept byte-identical by `tests/unit/test_skill_bundle_sync.py`. The Python SDK is not
vendored: the broker depends on the `sessionweave-sdk` package and `/sdk` renders its
installed README.

To refresh the third-party assets, re-download the pinned versions above from
`cdn.jsdelivr.net/npm/<project>@<version>/` and update this file.
