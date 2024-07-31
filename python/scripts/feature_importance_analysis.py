import random
import polars.selectors as cs
import polars as pl


from common.cast_frame import cast_frame, add_computed_cols
from bpdb import set_trace as s  # noqa: F401

from pve_rating import group_games_gamesettings
from common.common import grouped_gamesettings_preprocessor

# feature importance analysis
games = add_computed_cols(
    cast_frame(pl.read_parquet('replays_gamesettings.parquet')).filter(
        'barbarian' & ~pl.col('scavengers') & ~pl.col('raptors')
    )
)
grouped_games, _, _ = group_games_gamesettings(games, 'Raptors')
# Separate features and target
X = grouped_games.drop('Difficulty', '#Winners', '#Players')
y = grouped_games['Difficulty']

# Identify categorical and numerical columns
categorical_cols = X.select(cs.string()).columns
numerical_cols = X.select(cs.numeric()).columns

preprocessor = grouped_gamesettings_preprocessor(numerical_cols, categorical_cols)

# Apply transformations
X_preprocessed = preprocessor.fit_transform(X)

# Get feature names after preprocessing
categorical_feature_names = (
    preprocessor.transformers_[1][1].get_feature_names_out(categorical_cols).tolist()
)
feature_names = numerical_cols + categorical_feature_names

# correlation analysis
from scipy.stats import pearsonr
from sklearn.feature_selection import f_regression

X_pd = X.to_pandas()
y_pd = y.to_pandas()

# Correlation for numerical features
numerical_corr = {col: pearsonr(X_pd[col], y_pd)[0] for col in numerical_cols}

# Combine all preprocessed features into a single DataFrame
X_preprocessed_df = pl.DataFrame(
    X_preprocessed.toarray() if hasattr(X_preprocessed, 'toarray') else X_preprocessed
)
X_preprocessed_df.columns = feature_names

# Correlation for categorical features using ANOVA F-test
X_encoded = X_preprocessed_df.select(categorical_feature_names)
anova_f, _ = f_regression(X_encoded.to_pandas(), y_pd)

# Combine both results
correlation_values = list(numerical_corr.values()) + list(anova_f)

# Create a DataFrame for easy viewing
correlation_df = pl.DataFrame(
    {'Feature': feature_names, 'Difficulty Correlation': correlation_values}
)
correlation_df = correlation_df.with_columns(
    pl.col('Difficulty Correlation').fill_null(0)
)  # Replace NaN correlations with 0
correlation_df = correlation_df.sort('Difficulty Correlation', descending=True)

pl.Config.set_tbl_rows(1000)
pl.Config(fmt_table_cell_list_len=-1, fmt_str_lengths=1001)

from sklearn.ensemble import RandomForestRegressor

# Train a random forest model
model = RandomForestRegressor(
    min_samples_leaf=10,
    max_features=10000,
    random_state=random.seed(),
)
model.fit(X_preprocessed, y)

# Get feature importances
importances = model.feature_importances_
feature_names = (
    preprocessor.transformers_[0][1].get_feature_names_out(numerical_cols).tolist()
    + preprocessor.transformers_[1][1].get_feature_names_out(categorical_cols).tolist()
)

feature_importances = pl.DataFrame(
    zip(feature_names, importances), schema=['Feature', 'random forest regression']
)
print(
    feature_importances.join(correlation_df, on='Feature').sort(
        by='random forest regression', descending=True
    )
)

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(
    X_preprocessed,
    y,
    test_size=0.2,
    # random_state=123
)

# Train model for evaluation
model = RandomForestRegressor(
    min_samples_leaf=10,
    max_features=10000,
    random_state=random.seed(),
)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
print('Mean Squared Error:', mse)

s()
