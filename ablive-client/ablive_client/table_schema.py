from attrs import Factory, define

__all__ = ('Table', 'Schema')


@define(auto_attribs=True, frozen=True, slots=True, kw_only=True)
class Schema:
    fields: dict[str, str]
    fields_opt: str = Factory(str)
    create_opt: str = Factory(str)


class Table:
    def __init__(self, name: str, schema: Schema, rows_type: str = "list"):
        self.schema = schema
        self._name = name
        self._rows_type = rows_type
        self.sql_insert = ""
        self.sql_create = ""
        self._gen_sql()

    @property
    def name(self):
        return self._name

    def _gen_sql(self):
        self._gen_sql_insert()
        self._gen_sql_create()

    def _gen_sql_insert(self):
        table_name = self.name
        schema = self.schema
        if self._rows_type == "list":
            sql_insert = f"""
                INSERT INTO `{table_name}`
                    ({','.join(schema.fields.keys())})
                VALUES
                    ({','.join('%s' * len(schema.fields))})
            """
        elif self._rows_type == "dict":
            sql_insert = f"""
                INSERT INTO `{table_name}`
                    ({','.join(schema.fields.keys())})
                VALUES
                    ({','.join(f'%({field})s' for field in self.schema.fields.keys())})
            """
        elif self._rows_type == "sqlalchemy":
            sql_insert = f"""
                INSERT INTO `{table_name}`
                    ({','.join(schema.fields.keys())})
                VALUES
                    ({','.join(f':{field}' for field in self.schema.fields.keys())})
            """
        else:
            raise Exception("not a valid schema type")

        self.sql_insert = sql_insert

    def _gen_sql_create(self):
        fields_list = [f"{k} {v}" for k, v in self.schema.fields.items()]
        fields_list.append(self.schema.fields_opt)
        fields_str = ",".join(fields_list)

        self.sql_create = f"""
            CREATE TABLE IF NOT EXISTS `{self.name}` (
                {fields_str}
            ) {self.schema.create_opt};
        """
