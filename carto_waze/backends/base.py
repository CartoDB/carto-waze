def with_datasource(method):
    def method_wrapper(self, *args, **kwargs):
        with self.get_datasource() as datasource:
            return method(self, datasource, *args, **kwargs)

    return method_wrapper


def with_filter(method):
    def method_wrapper(self, *args, **kwargs):
        filter = []

        for filter_left, value in kwargs.items():
            try:
                field, operator = filter_left.split("__")
            except ValueError:
                field, operator = filter_left, "="
            else:
                if operator == "eq":
                    operator = "="
                elif operator == "neq":
                    operator = "!="
                elif operator == "gt":
                    operator = ">"
                elif operator == "gte":
                    operator = ">="
                elif operator == "lt":
                    operator = "<"
                elif operator == "lte":
                    operator = "<="
            filter.append(field + operator + str(value))
        return method(self, filter, *args)

    return method_wrapper


class Backend:
    fields = None

    def __init__(self):
        self.datasource = None

    def get_datasource(self):
        return self.datasource

    def get_values(self):
        raise NotImplementedError
