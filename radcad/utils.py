def flatten(nested_list):
    def generator(nested_list):
        for sublist in nested_list:
            if isinstance(sublist, list):
                for item in sublist:
                    yield item
            else:
                yield sublist

    return list(generator(nested_list))


def extract_exceptions(results_with_exceptions):
    results, exceptions = zip(*results_with_exceptions)
    return (list(flatten(flatten(list(results)))), list(exceptions))


TEXT_COLOR_RED = "\033[31;1m"
TEXT_COLOR_GREEN = "\033[32;1m"
TEXT_COLOR_YELLOW = "\033[33;1m"
TEXT_COLOR_BLUE = "\033[34;1m"
TEXT_COLOR_MAGENTA = "\033[35;1m"
TEXT_COLOR_CYAN = "\033[36;1m"
TEXT_COLOR_WHITE = "\033[37;1m"
TEXT_COLOR_DEFAULT = "\033[0m"
