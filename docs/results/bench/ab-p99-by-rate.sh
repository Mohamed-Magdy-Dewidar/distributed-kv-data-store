#!/bin/sh
# Draws GKE session 3's group commit A/B, the client Put p99 (k6) at each
# target rate, from the steps.csv of its three write ramps: off
# (20261017-180827), on (20261017-183051) and on past 900/s
# (20261017-194306). The y axis is logarithmic: off's last step is 100
# times its first. Plain awk, no plotting library; from the repository
# root:
#
#   sh docs/results/bench/ab-p99-by-rate.sh
#
# writes docs/images/gke-session3/ab-p99-by-rate.svg (or the path given).
set -eu

B=docs/results/bench
OUT=${1:-docs/images/gke-session3/ab-p99-by-rate.svg}

awk -F, '
function x(rate) { return 80 + rate / 1600 * 640 }
function y(ms) { return 60 + (log(5000) - log(ms)) / (log(5000) - log(5)) * 300 }
FNR == 1 {
	run++
	side[run] = FILENAME ~ /180827/ ? "off" : "on"
	for (i = 1; i <= NF; i++) col[$i] = i
	next
}
{
	n[run]++
	rate[run, n[run]] = $col["target_rate"]
	p99[run, n[run]] = $col["p99_ms"]
	errs[run, n[run]] = $col["errors"]
	ach[run, n[run]] = $col["achieved_rate"]
}
END {
	color["off"] = "#c0392b"; color["on"] = "#1f6feb"
	print "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"760\" height=\"500\" viewBox=\"0 0 760 500\" font-family=\"Helvetica, Arial, sans-serif\" font-size=\"12\">"
	print "<rect width=\"760\" height=\"500\" fill=\"#ffffff\"/>"
	print "<text x=\"380\" y=\"28\" text-anchor=\"middle\" font-size=\"15\" font-weight=\"bold\" fill=\"#222\">GKE session 3: client Put p99 by target rate, group commit off and on</text>"
	# y grid and labels (ms, log scale)
	split("5 10 20 50 100 200 500 1000 2000 5000", yt, " ")
	split("5|10|20|50|100|200|500|1,000|2,000|5,000", yl, "|")
	for (i = 1; i <= 10; i++) {
		printf "<line x1=\"80\" x2=\"720\" y1=\"%.1f\" y2=\"%.1f\" stroke=\"#e3e3e3\"/>\n", y(yt[i]), y(yt[i])
		printf "<text x=\"72\" y=\"%.1f\" text-anchor=\"end\" dominant-baseline=\"middle\" fill=\"#444\">%s</text>\n", y(yt[i]), yl[i]
	}
	# x grid and labels (Puts/s)
	split("0|200|400|600|800|1,000|1,200|1,400|1,600", xl, "|")
	for (i = 0; i <= 8; i++) {
		printf "<line x1=\"%.1f\" x2=\"%.1f\" y1=\"60\" y2=\"360\" stroke=\"#e3e3e3\"/>\n", x(i * 200), x(i * 200)
		printf "<text x=\"%.1f\" y=\"378\" text-anchor=\"middle\" fill=\"#444\">%s</text>\n", x(i * 200), xl[i + 1]
	}
	print "<rect x=\"80\" y=\"60\" width=\"640\" height=\"300\" fill=\"none\" stroke=\"#888\"/>"
	print "<text x=\"400\" y=\"402\" text-anchor=\"middle\" fill=\"#222\">Target write rate (Puts/s)</text>"
	print "<text transform=\"translate(22 210) rotate(-90)\" text-anchor=\"middle\" fill=\"#222\">Client Put p99 (ms, log scale)</text>"
	# one polyline per run; the two on runs are not joined (no step between 500 and 900/s)
	for (r = 1; r <= run; r++) {
		pts = ""
		for (i = 1; i <= n[r]; i++) pts = pts sprintf("%.1f,%.1f ", x(rate[r, i]), y(p99[r, i]))
		printf "<polyline points=\"%s\" fill=\"none\" stroke=\"%s\" stroke-width=\"2\"/>\n", pts, color[side[r]]
		for (i = 1; i <= n[r]; i++) {
			if (side[r] == "off")
				printf "<circle cx=\"%.1f\" cy=\"%.1f\" r=\"4\" fill=\"%s\"/>\n", x(rate[r, i]), y(p99[r, i]), color["off"]
			else
				printf "<rect x=\"%.1f\" y=\"%.1f\" width=\"8\" height=\"8\" fill=\"%s\"/>\n", x(rate[r, i]) - 4, y(p99[r, i]) - 4, color["on"]
			if (side[r] == "off" && errs[r, i] > 0)
				printf "<text x=\"%.1f\" y=\"%.1f\" fill=\"%s\">%s ms: %s/s achieved, %s errors</text>\n", x(rate[r, i]) + 10, y(p99[r, i]) + 4, color["off"], p99[r, i], ach[r, i], errs[r, i]
		}
	}
	# legend
	print "<circle cx=\"100\" cy=\"426\" r=\"4\" fill=\"" color["off"] "\"/><line x1=\"90\" x2=\"110\" y1=\"426\" y2=\"426\" stroke=\"" color["off"] "\" stroke-width=\"2\"/>"
	print "<text x=\"118\" y=\"430\" fill=\"#222\">Group commit off (ramp 20261017-180827, 60 s steps)</text>"
	print "<rect x=\"96\" y=\"444\" width=\"8\" height=\"8\" fill=\"" color["on"] "\"/><line x1=\"90\" x2=\"110\" y1=\"448\" y2=\"448\" stroke=\"" color["on"] "\" stroke-width=\"2\"/>"
	print "<text x=\"118\" y=\"452\" fill=\"#222\">Group commit on (ramps 20261017-183051, 60 s steps, and 20261017-194306, 15 s steps)</text>"
	print "<text x=\"118\" y=\"470\" fill=\"#666\">No run between 500 and 900/s is plotted. Same GKE cluster, commit e406077, 6 x e2-standard-2 in europe-west1;</text>"
	print "<text x=\"118\" y=\"488\" fill=\"#666\">each point is one step of k6 (one pod), from the run directory steps.csv.</text>"
	print "</svg>"
}' "$B"/20261017-180827-gke-writes-ramp/steps.csv \
	"$B"/20261017-183051-gke-writes-ramp/steps.csv \
	"$B"/20261017-194306-gke-writes-ramp/steps.csv >"$OUT"
echo "wrote $OUT"
