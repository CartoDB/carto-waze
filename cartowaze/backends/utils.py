import re


def to_snake_case(name):
    """
    thanks https://stackoverflow.com/a/1176023/3647833
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
