import numpy as np
import pandas as pd

df = pd.read_csv('tag_data/tag_data_calc.csv')


''' Group data '''
# Example groupby
grouped = df.groupby('participant_id')

''' Create sequences '''
def create_sequences(df, time_steps):
    X, y = [], []
    for _, group in df.groupby('participant_id'):
        metrics = group.drop(columns=['timestamp', 'binary_label', 'participant_id']).values
        labels = group['binary_label'].values

        # Create sequences
        for i in range(len(group) - time_steps + 1):
            X.append(metrics[i:i + time_steps])
            y.append(labels[i + time_steps - 1])  # Predict the label at the end of the sequence
    return np.array(X), np.array(y)

# Define the sequence length
time_steps = 10  # Example: 10 windows (300 seconds or 5 minutes)
X, y = create_sequences(df, time_steps)


''' Normalize features '''
from sklearn.preprocessing import StandardScaler

# Flatten X for normalization
num_features = X.shape[2]
reshaped_X = X.reshape(-1, num_features)
scaler = StandardScaler()
scaled_X = scaler.fit_transform(reshaped_X)

# Reshape back to sequences
X_normalized = scaled_X.reshape(X.shape)

''' Split data '''
from sklearn.model_selection import train_test_split

# Stratify by participant_id to maintain class balance (if needed)
train_idx, temp_idx = train_test_split(
    np.arange(len(y)), test_size=0.3, stratify=df['participant_id'][time_steps-1:].values, random_state=42
)
val_idx, test_idx = train_test_split(
    temp_idx, test_size=0.5, stratify=df['participant_id'][time_steps-1:].values[temp_idx], random_state=42
)

X_train, y_train = X_normalized[train_idx], y[train_idx]
X_val, y_val = X_normalized[val_idx], y[val_idx]
X_test, y_test = X_normalized[test_idx], y[test_idx]


''' Train model '''
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Bidirectional, LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping

# Define the model
model = Sequential([
    Bidirectional(LSTM(64, return_sequences=True, activation='tanh'), input_shape=(time_steps, num_features)),
    Dropout(0.3),
    Bidirectional(LSTM(32, activation='tanh')),
    Dropout(0.3),
    Dense(1, activation='sigmoid')  # Binary classification
])

# Compile the model
model.compile(optimizer=Adam(learning_rate=0.001), loss='binary_crossentropy', metrics=['accuracy'])

# Train the model
early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
history = model.fit(
    X_train, y_train,
    validation_data=(X_val, y_val),
    epochs=50,
    batch_size=32,
    callbacks=[early_stopping]
)

# Evaluate the model
test_loss, test_accuracy = model.evaluate(X_test, y_test, verbose=0)
print(f"Test Loss: {test_loss:.4f}, Test Accuracy: {test_accuracy:.4f}")


''' Interpret results '''
from sklearn.metrics import classification_report

y_pred = (model.predict(X_test) > 0.5).astype("int32")
print(classification_report(y_test, y_pred))
