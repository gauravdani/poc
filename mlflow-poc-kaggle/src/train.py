import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import kaggle

def load_data(data_path):
    # Download Titanic dataset from Kaggle
    # kaggle.api.dataset_download_files('titanic', path=data_path, unzip=True)
    
    # Load the data
    df = pd.read_csv(f"{data_path}/train.csv")
    return df

def preprocess_data(df):
    # Simple preprocessing
    df = df.drop(['Name', 'Ticket', 'Cabin', 'PassengerId'], axis=1)
    df = pd.get_dummies(df, columns=['Sex', 'Embarked'])
    df = df.fillna(df.mean())
    
    X = df.drop('Survived', axis=1)
    y = df['Survived']
    return train_test_split(X, y, test_size=0.2, random_state=42)

def train_model(X_train, X_test, y_train, y_test):
    with mlflow.start_run():
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        mlflow.log_param("n_estimators", 100)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.sklearn.log_model(model, "random_forest_model")
        
        print(f"Model accuracy: {accuracy}")

if __name__ == "__main__":
    data_path = "data/"
    df = load_data(data_path)
    X_train, X_test, y_train, y_test = preprocess_data(df)
    train_model(X_train, X_test, y_train, y_test)