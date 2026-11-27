#!/usr/bin/env python3
"""A static dashboard of the nightly tier, generated from `analysis.json` (nightly_analysis.py).

One page, no script and no request to any other host: the charts are inline SVG drawn here, the
style is inline, the only links are to this repository's commits and tags. Per benchmark it shows
the judged series, CPU time and allocation, and the wall-clock series marked as informational
(a hosted runner's neighbours move wall-clock time, so it is never judged there). Every chart
carries the change points of both detectors and the releases (git tags) that fall in its range.

    nightly_dashboard.py --analysis analysis.json --out DIR [--repository OWNER/NAME]

Writes DIR/index.html and DIR/analysis.json (the data behind the page, for anyone who wants to plot
it differently). Standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import html
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import bench_trend  # noqa: E402

WIDTH, HEIGHT = 640, 190
LEFT, RIGHT, TOP, BOTTOM = 84, 14, 12, 28
JUDGED_ORDER = ("cpu_ns_per_op", "alloc_bytes_per_op")
TITLES = {
    "cpu_ns_per_op": "CPU time per operation",
    "alloc_bytes_per_op": "Allocation per operation",
}
STATE_CLASS = {
    bench_trend.DRIFT: "drift",
    bench_trend.UNCONFIRMED: "unconfirmed",
    bench_trend.FASTER: "faster",
    bench_trend.FASTER_UNCONFIRMED: "faster",
    bench_trend.UNSTABLE: "unstable",
}

STYLE = """
:root{color-scheme:light dark;--bg:#fbfbfa;--fg:#1c1c1a;--muted:#62625c;--card:#fff;--line:#d9d8d2;--accent:#1f6f8b;
--drift:#b3261e;--unconfirmed:#a15c00;--faster:#0b7a53;--unstable:#7a5aa6;--info:#8a8a82;--release:#4a4a44}
@media (prefers-color-scheme:dark){:root{--bg:#161614;--fg:#ecebe6;--muted:#a3a29a;--card:#1f1f1c;--line:#3a3a35;
--accent:#6cc4e0;--drift:#ff8a80;--unconfirmed:#ffb454;--faster:#5fd3a2;--unstable:#c2a6ea;--info:#8f8f86;--release:#c9c8c0}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 64px}h1{font-size:1.6rem;margin:.2em 0}h2{font-size:1.15rem;margin:0}
p.lead,.muted{color:var(--muted)}a{color:var(--accent)}code{font:13px ui-monospace,monospace}
.banner{border:1px solid var(--line);border-left:4px solid var(--info);background:var(--card);padding:10px 14px;margin:16px 0;border-radius:6px}
table{border-collapse:collapse;width:100%;background:var(--card);border:1px solid var(--line);border-radius:6px}
th,td{text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);vertical-align:top;font-size:14px}
th{font-weight:600;color:var(--muted)}td.num{font-variant-numeric:tabular-nums;white-space:nowrap}
.pill{display:inline-block;padding:0 8px;border-radius:10px;border:1px solid currentColor;font-size:12px}
.pill.drift{color:var(--drift)}.pill.unconfirmed{color:var(--unconfirmed)}.pill.faster{color:var(--faster)}
.pill.unstable{color:var(--unstable)}.pill.steady,.pill.insufficient-history{color:var(--muted)}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin:18px 0}
.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,520px),1fr));gap:14px;margin-top:10px}
figure{margin:0;min-width:0}figcaption{font-size:13px;color:var(--muted);margin-bottom:2px}
figcaption strong{color:var(--fg)}svg{width:100%;height:auto;display:block}
svg text{fill:var(--muted);font-size:11px}svg .grid{stroke:var(--line);stroke-width:1}
svg .line{fill:none;stroke:var(--accent);stroke-width:1.8}svg .info .line{stroke:var(--info);stroke-dasharray:4 3}
svg .dot{fill:var(--accent)}svg .info .dot{fill:var(--info)}
svg .dot.drift{fill:var(--drift)}svg .dot.unconfirmed{fill:var(--unconfirmed)}svg .dot.faster{fill:var(--faster)}
svg .release{stroke:var(--release);stroke-width:1;stroke-dasharray:1 3}
svg .cp-conv-304{stroke:var(--drift);stroke-width:1.6}svg .cp-otava{stroke:var(--unconfirmed);stroke-width:1.6;stroke-dasharray:5 3}
svg .cp-down{stroke:var(--faster)}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;font-size:13px;color:var(--muted);margin:8px 0}
.legend i{display:inline-block;width:22px;height:0;border-top:2px solid;vertical-align:middle;margin-right:5px}
footer{margin-top:36px;color:var(--muted);font-size:13px}
"""


def _date(night: str) -> datetime.date:
    return datetime.date.fromisoformat(night)


def title_of(metric: str) -> str:
    return TITLES.get(metric, "Wall-clock time per operation" if metric.endswith("_per_op") else metric)


def format_value(value: float, unit: str) -> str:
    """A level with a unit that fits it: ns/op as ns, µs, ms or s; B/op as B, KiB, MiB, GiB."""
    if unit == "ns/op":
        for limit, divisor, suffix in ((1e9, 1e9, "s"), (1e6, 1e6, "ms"), (1e3, 1e3, "µs")):
            if abs(value) >= limit:
                return f"{value / divisor:.4g} {suffix}/op"
        return f"{value:.4g} ns/op"
    if unit == "B/op":
        for limit, divisor, suffix in ((2**30, 2**30, "GiB"), (2**20, 2**20, "MiB"), (2**10, 2**10, "KiB")):
            if abs(value) >= limit:
                return f"{value / divisor:.4g} {suffix}/op"
        return f"{value:.4g} B/op"
    return f"{value:.4g} {unit}".strip()


def _nice_bounds(low: float, high: float) -> tuple[float, float]:
    if high - low <= 1e-12 * max(abs(high), 1.0):
        pad = abs(high) * 0.05 or 1.0
        return low - pad, high + pad
    pad = (high - low) * 0.12
    return max(0.0, low - pad) if low >= 0 else low - pad, high + pad


def chart_svg(series: Mapping[str, Any], releases: Sequence[Mapping[str, str]]) -> str:
    """One series as an inline SVG: the nights' levels, state-coloured, with change and release markers."""
    nights = series["nights"]
    info = " info" if series["informational"] else ""
    label = f"{title_of(series['metric'])}, {series['name']}"
    if not nights:
        return f'<svg viewBox="0 0 {WIDTH} 40" role="img" aria-label="{html.escape(label)}"><text x="8" y="22">no recorded night</text></svg>'
    days = [_date(entry["night"]).toordinal() for entry in nights]
    first, last = days[0], days[-1]
    span = max(last - first, 1)
    low, high = _nice_bounds(min(e["level"] for e in nights), max(e["level"] for e in nights))
    plot_w, plot_h = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM

    def x_of(day: int) -> float:
        return LEFT + plot_w * ((day - first) / span if last > first else 0.5)

    def y_of(value: float) -> float:
        return TOP + plot_h * (1 - (value - low) / (high - low))

    out = [
        f'<svg viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{html.escape(label)}" class="chart{info}">',
        f"<title>{html.escape(label)}</title>",
    ]
    for step in range(5):
        value = low + (high - low) * step / 4
        y = y_of(value)
        out.append(f'<line class="grid" x1="{LEFT}" y1="{y:.1f}" x2="{WIDTH - RIGHT}" y2="{y:.1f}"/>')
        out.append(f'<text x="{LEFT - 6}" y="{y + 4:.1f}" text-anchor="end">{html.escape(format_value(value, series["unit"]))}</text>')
    for day in sorted({first, first + span // 2, last}):
        # The outer labels are anchored inside the plot so that neither is cut off by the edge of the chart.
        anchor = "start" if day == first and last > first else "end" if day == last and last > first else "middle"
        out.append(
            f'<text x="{x_of(day):.1f}" y="{HEIGHT - 8}" text-anchor="{anchor}">'
            f"{datetime.date.fromordinal(day).isoformat()}</text>"
        )
    for release in releases:
        day = _date(release["date"]).toordinal()
        if first <= day <= last:
            x = x_of(day)
            out.append(
                f'<line class="release" x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" y2="{TOP + plot_h}"/>'
                f'<text x="{x + 3:.1f}" y="{TOP + 9}" >{html.escape(release["tag"].removeprefix("sirix-"))}</text>'
            )
    for change in series["changes"]:
        x = x_of(_date(change["night"]).toordinal())
        for detector in change["detectors"]:
            klass = f"cp-{detector}" + (" cp-down" if change["direction"] == bench_trend.DOWNWARD else "")
            out.append(
                f'<line class="{klass}" x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" y2="{TOP + plot_h}">'
                f"<title>{html.escape(detector)}: {html.escape(change['previous_commit'][:12])}..{html.escape(change['commit'][:12])}"
                f"</title></line>"
            )
    points = " ".join(f"{x_of(day):.1f},{y_of(e['level']):.1f}" for day, e in zip(days, nights))
    out.append(f'<g class="series{info}"><polyline class="line" points="{points}"/>')
    for day, entry in zip(days, nights):
        klass = STATE_CLASS.get(entry["state"], "")
        radius = 3.6 if klass else 2.4
        klass = f"dot {klass}".strip()
        tip = (
            f"{entry['night']}: {format_value(entry['level'], series['unit'])} "
            f"(median of {entry['processes']}), {entry['state']}, commit {entry['commit'][:12]}"
        )
        out.append(
            f'<circle class="{klass}" cx="{x_of(day):.1f}" cy="{y_of(entry["level"]):.1f}" r="{radius}">'
            f"<title>{html.escape(tip)}</title></circle>"
        )
    out.append("</g></svg>")
    return "".join(out)


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _group_name(series: Mapping[str, Any]) -> str:
    params = ",".join(f"{name}={value}" for name, value in sorted(series["params"].items()))
    return f"{series['benchmark']}{{{params}}}" if params else series["benchmark"]


def _state_pill(state: str) -> str:
    return f'<span class="pill {STATE_CLASS.get(state, state)}">{html.escape(state)}</span>'


def _commit_link(repository: str | None, commit: str) -> str:
    short = html.escape(commit[:12])
    return f'<a href="https://github.com/{html.escape(repository)}/commit/{commit}">{short}</a>' if repository else f"<code>{short}</code>"


def _range_link(repository: str | None, change: Mapping[str, Any]) -> str:
    if not repository:
        return f"<code>{html.escape(change['previous_commit'][:12])}..{html.escape(change['commit'][:12])}</code>"
    href = f"https://github.com/{repository}/compare/{change['previous_commit']}...{change['commit']}"
    text = f"{change['previous_commit'][:12]}..{change['commit'][:12]}"
    return f'<a href="{html.escape(href)}">{html.escape(text)}</a>'


def _changes_table(analysis: Mapping[str, Any], repository: str | None) -> str:
    rows = []
    for series in analysis["series"]:
        for change in series["changes"]:
            rows.append((change["night"], series, change))
    if not rows:
        return '<p class="muted">No change point has been detected in any series.</p>'
    lines = [
        "<table><thead><tr><th>Night</th><th>Series</th><th>Direction</th><th>Detectors</th>"
        "<th>Change</th><th>Commits</th></tr></thead><tbody>"
    ]
    for night, series, change in sorted(rows, key=lambda item: item[0], reverse=True):
        relative = change["relative_change"]
        shown = "from zero" if relative is None else f"{100 * relative:+.1f}%"
        note = " <span class='muted'>(informational)</span>" if series["informational"] else ""
        agreement = (
            f" <span class='muted'>{html.escape(change['agreement'])}</span>"
            if analysis["otava"] and change["agreement"] != "both"
            else ""
        )
        lines.append(
            f"<tr><td class='num'>{html.escape(night)}</td>"
            f"<td><a href='#s-{series['id']}'>{html.escape(_group_name(series))}</a> "
            f"<span class='muted'>{html.escape(title_of(series['metric']))}</span>{note}</td>"
            f"<td>{'slower' if change['direction'] == bench_trend.UPWARD else 'faster'}</td>"
            f"<td>{html.escape(', '.join(change['detectors']))}{agreement}</td>"
            f"<td class='num'>{shown}</td><td>{_range_link(repository, change)}</td></tr>"
        )
    lines.append("</tbody></table>")
    return "".join(lines)


def _summary(analysis: Mapping[str, Any]) -> str:
    counts: dict[str, int] = {}
    for series in analysis["series"]:
        if series["informational"] or not series["nights"]:
            continue
        state = series["nights"][-1]["state"]
        counts[state] = counts.get(state, 0) + 1
    if not counts:
        return '<p class="muted">No judged series has a recorded night yet.</p>'
    order = (bench_trend.DRIFT, bench_trend.UNCONFIRMED, bench_trend.FASTER, bench_trend.FASTER_UNCONFIRMED,
             bench_trend.UNSTABLE, bench_trend.INSUFFICIENT, bench_trend.STEADY)  # fmt: skip
    pills = " ".join(f"{_state_pill(name)} {counts[name]}" for name in order if name in counts)
    return f"<p>Judged series on the latest night: {pills}</p>"


def _lead(analysis: Mapping[str, Any], repository: str | None) -> str:
    latest = html.escape(str(analysis["latest_night"] or "none yet"))
    if not repository:
        return f"<p class='lead'>Latest night: <strong>{latest}</strong>.</p>"
    rule = html.escape(f"https://github.com/{repository}/blob/main/spec/CONV-conventions.md#conv-304--nightly-trend-tier")
    otava = " with Apache Otava as a second opinion" if analysis["otava"] else ""
    return (
        f"<p class='lead'>Latest night: <strong>{latest}</strong>. CPU time and allocation per operation, recorded "
        f"every night in fresh processes on a GitHub-hosted runner and judged by the nightly trend rule "
        f"(<a href='{rule}'>CONV-304</a>){otava}.</p>"
    )


def render(analysis: Mapping[str, Any], *, repository: str | None = None, generated: str | None = None) -> str:
    """The whole page as text."""
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for series in analysis["series"]:
        groups.setdefault(_group_name(series), []).append(series)
    stamp = generated or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    page = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width,initial-scale=1'>",
        "<title>SirixDB nightly benchmarks</title>",
        f"<style>{STYLE}</style></head><body><main>",
        "<h1>SirixDB nightly benchmarks</h1>",
        _lead(analysis, repository),
        "<div class='banner'><strong>Wall-clock time is informational.</strong> A shared runner's neighbours move "
        "elapsed time, so wall-clock series are drawn dashed and grey, are never judged and never open an issue. "
        "CPU time and allocation are judged. Wall-clock time will be judged on a dedicated benchmark machine.</div>",
        _summary(analysis),
        "<div class='legend'>"
        "<span><i style='border-color:var(--drift)'></i>CONV-304 change (the judge)</span>"
        "<span><i style='border-color:var(--unconfirmed);border-top-style:dashed'></i>Apache Otava change point</span>"
        "<span><i style='border-color:var(--release);border-top-style:dotted'></i>release tag</span>"
        "<span><i style='border-color:var(--info);border-top-style:dashed'></i>informational series</span></div>",
        "<h2>Detected changes</h2>",
        _changes_table(analysis, repository),
        "<h2 style='margin-top:28px'>Benchmarks</h2>",
    ]
    releases = analysis["releases"]
    for name in sorted(groups):
        members = sorted(
            groups[name],
            key=lambda item: (
                JUDGED_ORDER.index(item["metric"]) if item["metric"] in JUDGED_ORDER else len(JUDGED_ORDER),
                item["conditions"],
            ),
        )
        page.append(f"<section class='card' id='b-{_slug(name)}'><h2>{html.escape(name)}</h2><div class='charts'>")
        for series in members:
            latest = series["nights"][-1] if series["nights"] else None
            title = title_of(series["metric"])
            tag = " <span class='pill'>informational, not judged</span>" if series["informational"] else ""
            current = (
                f" &mdash; {html.escape(format_value(latest['level'], series['unit']))} {_state_pill(latest['state'])}"
                if latest
                else ""
            )
            page.append(
                f"<figure id='s-{series['id']}'><figcaption><strong>{html.escape(title)}</strong>{tag}{current}<br>"
                f"{html.escape(series['conditions'])}, {html.escape(series['binary'])}, "
                f"{html.escape(series['runner'])} runner</figcaption>{chart_svg(series, releases)}</figure>"
            )
        page.append("</div></section>")
    page.append(
        f"<footer>Generated {html.escape(stamp)} from the nightly ledger on the <code>nightly-data</code> branch. "
        f"The data behind every chart is <a href='analysis.json'>analysis.json</a>. "
        f"No script runs on this page and it makes no request to any other site.</footer></main></body></html>"
    )
    return "\n".join(page)


def write_site(analysis: Mapping[str, Any], out: Path, *, repository: str | None = None, generated: str | None = None) -> None:
    """Write index.html, analysis.json and a .nojekyll marker into a directory, replacing nothing else."""
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(render(analysis, repository=repository, generated=generated), encoding="utf-8")
    (out / "analysis.json").write_text(json.dumps(analysis, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render the nightly dashboard.")
    parser.add_argument("--analysis", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--repository", help="OWNER/NAME, for commit and tag links")
    args = parser.parse_args(argv)
    try:
        analysis = json.loads(args.analysis.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"nightly_dashboard.py: {error}", file=sys.stderr)
        return 2
    if args.repository and not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository):
        print("nightly_dashboard.py: --repository must be OWNER/NAME", file=sys.stderr)
        return 2
    write_site(analysis, args.out, repository=args.repository)
    print(f"wrote {args.out / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
