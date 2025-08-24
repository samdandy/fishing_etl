def grade_flow_rate(flow_rate):
    if 0 >= flow_rate < 1200:
        return 4
    elif 1200 <= flow_rate < 2200:
        return 3
    elif 2200 <= flow_rate < 3700:
        return 2
    elif flow_rate >= 3700:
        return 1