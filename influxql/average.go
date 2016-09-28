package influxql

func averageFloat(wt, lt, nt int64, prev, next float64) float64 {
	windowTime := float64(wt)
	lastTime := float64(lt)
	nextTime := float64(nt)
	return prev + ((next-prev)/(nextTime-lastTime))*(windowTime-lastTime)
}

func averageInteger(wt, lt, nt int64, prev, next int64) int64 {
	return prev + ((next-prev)/(nt-lt))*(wt-lt)
}

func averageString(wt, lt, nt int64, prev, next string) string {
	return ""
}

func averageBoolean(wt, lt, nt int64, prev, next bool) bool {
	return false
}
