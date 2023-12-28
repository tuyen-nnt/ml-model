from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score

def trainModel(df):
    # Display information about missing values
    print("Missing values before handling:")
    df = df.astype({'request':'string','response':'int'})
    print(df.request.dtypes)

    # Separate the target variable and features
    X = df.drop('response', axis=1)  # Replace 'type' with the name of your target variable
    y = df['response']
    # X = dataset.iloc[:, :-1].values
    # y = dataset.iloc[:, -1].values

    # Use LabelEncoder for the target variable
    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(y)

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(y_train)
    print(y_test)
    # Calculate frequencies of each category in the 'url' column in the training set
    request_frequencies = X_train['request'].value_counts(normalize=True)

    # Replace categories with their frequencies in both training and testing sets
    X_train['request_frequency'] = X_train['request'].map(request_frequencies)
    X_test['request_frequency'] = X_test['request'].map(request_frequencies)
    print("Replaced frequency")

    # Drop the original 'url' column
    X_train = X_train.drop('request', axis=1)
    X_test = X_test.drop('request', axis=1)

    # Now drop NaN values from y_test and corresponding rows from X_test
    # X_test_no_nan = X_test.dropna()
    # y_test = y_test.loc[X_test_no_nan.index]
    # y_test_no_nan = y_test.dropna()
    # X_test = X_test.loc[y_test_no_nan.index]
    X_test_no_missing = X_test.dropna()
    y_test_no_missing = y_test[X_test.index.isin(X_test_no_missing.index)]


    # Drop rows with missing values
    X_train_no_missing = X_train.dropna()
    y_train_no_missing = y_train[X_train.index.isin(X_train_no_missing.index)]

    # Train a model (Random Forest Classifier in this example)
    model = RandomForestClassifier(n_estimators = 20, criterion = 'gini', random_state = 0)
    # model = RandomForestClassifier(n_estimators = 20, criterion = 'entropy', random_state = 0)

    # Train the model
    model.fit(X_train_no_missing, y_train_no_missing)
    print("Trained")

    # Make predictions on the test set
    y_pred = model.predict(X_test_no_missing)

    # Evaluate the model
    accuracy = accuracy_score(y_test_no_missing, y_pred)
    print(f"Accuracy: {accuracy}")
    return model