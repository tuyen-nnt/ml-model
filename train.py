from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np


def trainModel(df):
    # Display information about missing values
    print("Missing values before handling:")
    df = df.astype({'request':'string','response':'int'})
    # print(df.request.dtypes)

    # Separate the target variable and features
    X = df["request"]  # Replace 'type' with the name of your target variable
    y = df['response']

    # Use LabelEncoder for the target variable
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(y)
    print(y)
    label_decoder = str(label_encoder.inverse_transform([1]))
    print(label_decoder)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(X_train.info())

    # Reset index before adding new value
    X_train = X_train.reset_index(drop=True)
    # Add a new value to the pandas series X and numpy array y_train
    new_value = 'No'
    print(len(X_train))
    X_train.loc[len(X_train)] = new_value
    # Print the last row separately
    print("Last Row:")
    print(len(X_train))
    new_value = 1
    y_train = np.append(y_train, new_value)

    # Calculate frequencies of each category in the 'request' column in the training set
    request_frequencies = X_train.value_counts(normalize=True)
    # request_frequencies = request_frequencies
    print("request_frequencies")
    print(request_frequencies)
    # Changing the value of an element by index
    index_to_change = 'No'
    new_value = 0
    request_frequencies[index_to_change] = new_value

    # Replace categories with their frequencies in both training and testing sets
    X_train_freq = X_train.map(request_frequencies)
    print("X_train_freq")
    print(X_train_freq)
    X_test_freq = X_test.map(request_frequencies)

    # Add a new column with the frequencies to the original DataFrame
    X_train_with_freq = pd.concat([X_train, X_train_freq.rename('request_frequency')], axis=1)
    print("X_train_with_freq")
    print(X_train_with_freq)

    X_test_with_freq = pd.concat([X_test, X_test_freq.rename('request_frequency')], axis=1)

    # Drop the original 'request' column
    X_train_with_freq = X_train_with_freq.drop(labels='request', axis=1)
    X_test_with_freq = X_test_with_freq.drop(labels='request', axis=1)

    # Count the number of null values
    null_count = X_test_with_freq.isnull().sum()
    print("null_count")
    print(null_count)

    # Convert y_test to a pandas Series
    y_test = pd.Series(y_test)
    # Replace NaN values
    X_test_no_nan = X_test_with_freq.fillna(0)
    print(len(X_test_no_nan))
    # y_test = y_test.loc[X_test_no_nan.index]
    print(len(y_test))

    # Train a model (Random Forest Classifier in this example)
    model = RandomForestClassifier(n_estimators = 20, criterion = 'gini', random_state = 0)
    print(f"X train is: {X_train_with_freq}")
    # Train the model
    model.fit(X_train_with_freq, y_train)
    print("Trained")

    # Make predictions on the test set
    y_pred = model.predict(X_test_no_nan)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy}")
    print(y_test)
    return request_frequencies, model, label_encoder