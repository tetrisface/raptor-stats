from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def grouped_gamesettings_preprocessor(numerical_cols, categorical_cols):
    # Preprocessing for numerical data
    numerical_transformer = Pipeline(
        memory=None,
        steps=[
            ('imputer', SimpleImputer(strategy='mean')),
            ('scaler', StandardScaler()),
        ],
    )

    # Preprocessing for categorical data
    categorical_transformer = Pipeline(
        memory=None,
        steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore')),
        ],
    )

    # Combine preprocessing steps
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, numerical_cols),
            ('cat', categorical_transformer, categorical_cols),
        ]
    )
    return preprocessor
