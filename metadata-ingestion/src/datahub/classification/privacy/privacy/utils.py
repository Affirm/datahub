import collections


def nested_dict_iter(nested):
    for key, value in nested.items():
        if isinstance(value, collections.Mapping):
            for inner_key, inner_value in nested_dict_iter(value):
                yield inner_key, inner_value
        else:
            yield key, value