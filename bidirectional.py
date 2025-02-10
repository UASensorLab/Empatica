import numpy as np
import pandas as pd
import tensorflow as tf

SHUFFLE_BUFFER = 500
BATCH_SIZE = 2

empatica_train = pd.read_csv('./tag_data/tag_data_calc.csv')
print(empatica_train.head())

empatica_features = empatica_train.copy()
empatica_labels = empatica_features.pop("status")

inputs = {}

for name, column in empatica_features.items():
  dtype = column.dtype
  if dtype == object:
    dtype = tf.string
  else:
    dtype = tf.float32

  inputs[name] = tf.keras.Input(shape=(1,), name=name, dtype=dtype)

numeric_inputs = {name:input for name,input in inputs.items()
                  if input.dtype==tf.float32}

x = tf.keras.layers.Concatenate()(list(numeric_inputs.values()))
norm = tf.keras.layers.Normalization()
norm.adapt(np.array(empatica_train[numeric_inputs.keys()]))
all_numeric_inputs = norm(x)

preprocessed_inputs = [all_numeric_inputs]

for name, input in inputs.items():
  if input.dtype == tf.float32:
    continue

  lookup = tf.keras.layers.StringLookup(vocabulary=np.unique(empatica_features[name]))
  one_hot = tf.keras.layers.CategoryEncoding(num_tokens=lookup.vocabulary_size())

  x = lookup(input)
  x = one_hot(x)
  preprocessed_inputs.append(x)

preprocessed_inputs_cat = tf.keras.layers.Concatenate()(preprocessed_inputs)

empatica_preprocessing = tf.keras.Model(inputs, preprocessed_inputs_cat)

# tf.keras.utils.plot_model(model = empatica_preprocessing , rankdir="LR", dpi=72, show_shapes=True)

empatica_features_dict = {name: np.array(value) 
                         for name, value in empatica_features.items()}

features_dict = {name:values[:1] for name, values in empatica_features_dict.items()}
empatica_preprocessing(features_dict)

for name, value in empatica_features_dict.items():
    print(f"{name}: {np.array(value).dtype}")

for name, value in features_dict.items():
    print(f"{name}: {np.array(value).dtype}")


def empatica_model(preprocessing_head, inputs):
    body = tf.keras.Sequential([
        tf.keras.layers.Reshape((1, -1)),  # Add a time dimension
        tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(64, return_sequences=True)),
        tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(32)),
        tf.keras.layers.Dense(10, activation='softmax')
    ])

    preprocessed_inputs = preprocessing_head(inputs)
    result = body(preprocessed_inputs)
    model = tf.keras.Model(inputs, result)

    model.compile(loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                  optimizer=tf.keras.optimizers.Adam(), metrics=['accuracy'])
    return model


empatica_model = empatica_model(empatica_preprocessing, inputs)
empatica_model.fit(features_dict, empatica_labels, epochs=10, batch_size=32)