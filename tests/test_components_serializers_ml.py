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

    def test_TensorflowSavedModel(self):
        def transform_keras_model(
            model_path: InputPath("TensorflowSavedModel"),
            output_model_path: OutputPath("TensorflowSavedModel"),
        ):
            import tensorflow

            model = tensorflow.keras.models.load_model(filepath=model_path)
            print(model)
            tensorflow.keras.models.save_model(model=model, filepath=output_model_path)

        transform_keras_model_op = create_component_from_func(
            func=transform_keras_model,
            base_image="tensorflow/tensorflow:2.11.0",
        )

        import tensorflow as tf

        with runners.InteractiveMode():
            input_model = tf.keras.Sequential(
                [tf.keras.layers.Dense(5, input_shape=(3,)), tf.keras.layers.Softmax()]
            )
            output_art = transform_keras_model_op(model=input_model).outputs[
                "output_model"
            ]
            output_model = output_art.materialize()
            assert output_model
            assert output_model.signatures
            assert output_model.variables
            predictions = output_model(tf.constant([[0.1, 0.2, 0.3]]))
            assert predictions.shape == (1, 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
