import unittest

from cloud_pipelines.components import create_component_from_func, InputPath, OutputPath
from cloud_pipelines.orchestration import runners


class ComponentsSerializersMLTestCase(unittest.TestCase):
    def test_pandas_dataframe_materialization(self):
        def select_columns(
            table_path: InputPath("ApacheParquet"),
            output_table_path: OutputPath("ApacheParquet"),
            column_names: list,
        ):
            import pandas

            df = pandas.read_parquet(table_path)
            print("Input table:")
            df.info()
            df = df[column_names]
            print("Output table:")
            df.info()
            df.to_parquet(output_table_path, index=False)

        select_columns_op = create_component_from_func(
            func=select_columns,
            packages_to_install=["pandas==1.3.5", "pyarrow==10.0.1"],
        )

        import pandas

        with runners.InteractiveMode():
            input_df = pandas.DataFrame(
                {
                    "feature1": [1, 2, 3, 4, 5],
                    "feature2": [0.1, 0.2, 0.3, 0.4, 0.5],
                    "feature3": ["a", "b", "c", "d", "e"],
                }
            )
            output_art = select_columns_op(
                table=input_df, column_names=["feature3"]
            ).outputs["output_table"]
            output_df = output_art.materialize()
            assert output_df.columns.to_list() == ["feature3"]


if __name__ == "__main__":
    unittest.main(verbosity=2)
