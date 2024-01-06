from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np
import os
import logging

def trainModel(df,logger):

    # Load environment variables from .env file
    with open(".env") as env_file:
        for line in env_file:
            # Ignore lines starting with '#' as comments
            if not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value

    # Access environment variables
    feature_column = os.environ.get("FEATURE_COLUMN")
    target_column = os.environ.get("TARGET_COLUMN")

    # Display information about missing values
    logging.info("Missing values before handling:")
    df = df.astype({feature_column:'string', target_column:'int'})
    # print(df.request.dtypes)

    # Separate the target variable and features
    X = df[feature_column]  # Replace 'type' with the name of your target variable
    y = df[target_column]

    # Use LabelEncoder for the target variable
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(y)
    logging.info(y)
    label_decoder = str(label_encoder.inverse_transform([1]))
    logging.info(label_decoder)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    logging.info(X_train.info())

    # Reset index before adding new value to make prediction improved
    X_train = X_train.reset_index(drop=True)
    # Add a new value to the pandas series X and numpy array y_train
    X_train_new_value = 'No'
    X_train.loc[len(X_train)] = X_train_new_value
    y_train_new_value = 1
    y_train = np.append(y_train, y_train_new_value)

    # Calculate frequencies of each category in the 'request' column in the training set
    request_frequencies = X_train.value_counts(normalize=True)
    # request_frequencies = request_frequencies
    logging.info(f"request_frequencies: {request_frequencies}")
    # Changing the value of an element by index
    index_to_change = 'No'
    new_value = 0
    request_frequencies[index_to_change] = new_value

    # Replace categories with their frequencies in both training and testing sets
    X_train_freq = X_train.map(request_frequencies)
    logging.info(f"X_train_freq: {X_train_freq}")
    X_test_freq = X_test.map(request_frequencies)

    # Add a new column with the frequencies to the original DataFrame
    X_train_with_freq = pd.concat([X_train, X_train_freq.rename('request_frequency')], axis=1)
    logging.info(f"X_train_with_freq {X_train_with_freq}")

    X_test_with_freq = pd.concat([X_test, X_test_freq.rename('request_frequency')], axis=1)

    # Drop the original 'request' column
    X_train_with_freq = X_train_with_freq.drop(labels=feature_column, axis=1)
    X_test_with_freq = X_test_with_freq.drop(labels=feature_column, axis=1)

    # Count the number of null values
    null_count = X_test_with_freq.isnull().sum()
    logging.info(f"null_count: {null_count}")

    # Convert y_test to a pandas Series
    y_test = pd.Series(y_test)
    # Replace NaN values
    X_test_no_nan = X_test_with_freq.fillna(0)

    # Train a model (Random Forest Classifier in this example)
    model = RandomForestClassifier(n_estimators = 20, criterion = 'gini', random_state = 0)
    logging.info(f"X train is: {X_train_with_freq}")
    # Train the model
    model.fit(X_train_with_freq, y_train)
    logging.info("Trained")

    # Make predictions on the test set
    y_pred = model.predict(X_test_no_nan)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"Accuracy: {accuracy}")
    logging.info(y_test)
    return request_frequencies, model, label_encoder