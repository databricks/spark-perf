from math import sqrt


OUTPUT_DIVIDER_STRING = "-" * 68


def average(in_list):
    return sum(in_list) / len(in_list)


def variance(in_list):
    variance = 0
    for x in in_list:
        variance = variance + (average(in_list) - x) ** 2
    return variance / len(in_list)


def append_config_to_file(filename, java_opt_list, opt_list):
    """
    Append configuration of a test run to a log file
    """
    with open(filename, "a") as fout:
        fout.write(OUTPUT_DIVIDER_STRING + "\n")
        fout.write("Java options: %s\n" % (" ".join(java_opt_list)))
        fout.write("Options: %s\n" % (" ".join(opt_list)))
        fout.write(OUTPUT_DIVIDER_STRING + "\n")
        fout.flush()


def stats_for_results(result_list):
    assert len(result_list) > 0, "stats_for_results given empty result_list"
    result_first = result_list[0]
    result_last = result_list[-1]
    sorted_results = sorted([float(x) for x in result_list])
    result_med = sorted_results[len(sorted_results)/2]
    if (len(result_list) % 2 == 0):
        result_med = (sorted_results[len(result_list)/2-1] + sorted_results[len(result_list)/2])/2
    result_std = sqrt(variance(sorted_results))
    result_min = sorted_results[0]

    return (result_med, result_std, result_min, result_first, result_last)
