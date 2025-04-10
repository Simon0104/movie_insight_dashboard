import streamlit as st
import pandas as pd
import json
import plotly.express as px
from collections import Counter

# -------------------------------
# Data loading section (from pre-processed CSV)
# -------------------------------
@st.cache_data
def load_data(file_path="data/output_df10.csv"):
    # Assumes output_df10.csv contains the final cleaned, merged, and transformed dataset
    df = pd.read_csv("../data/output_df10.csv", index_col=0, parse_dates=["release_date"])
    return df

df = load_data()

st.title("Interactive Movie Analytics Dashboard")

# -------------------------------
# Sidebar filter (filter by release year)
# -------------------------------
st.sidebar.header("Data Filter")
years = df["release_date"].dt.year.dropna().unique()
years = sorted(years)
selected_years = st.sidebar.multiselect("Select release years", years, default=years)

# Make a copy to avoid SettingWithCopyWarning
if selected_years:
    df_filtered = df.loc[df["release_date"].dt.year.isin(selected_years)].copy()
else:
    df_filtered = df.copy()

st.write(f"Total entries: {df_filtered.shape[0]}")

# -------------------------------
# Visualization 1: Movie Genre Distribution (Pie Chart)
# -------------------------------
st.subheader("Movie Genre Distribution")

def parse_genre_list(x):
    """
    Convert raw genre field into a list of genre names:
    1. If x is a string, evaluate it into a Python object (assuming trusted data).
    2. If the result is a list, iterate through each item:
       - If item is a dict with a 'name' field, extract it.
       - If item is a string, use directly.
       - Otherwise convert item to string.
    3. Return empty list if none applies.
    """
    if isinstance(x, str):
        try:
            obj = eval(x)
        except:
            return []
    elif isinstance(x, list):
        obj = x
    else:
        return []

    if not isinstance(obj, list):
        return []

    genre_names = []
    for item in obj:
        if isinstance(item, dict) and "name" in item:
            genre_names.append(item["name"])
        elif isinstance(item, str):
            genre_names.append(item)
        else:
            genre_names.append(str(item))
    return genre_names

# Apply genre parser and store as 'genres_list'
df_filtered.loc[:, "genres_list"] = df_filtered["genres"].apply(parse_genre_list)

# Preview first 5 parsed results
st.write("genres_list (first 5 rows):")
st.write(df_filtered["genres_list"].head())

# Count genre frequency using Counter
all_genres = []
for g_list in df_filtered["genres_list"]:
    if isinstance(g_list, list):
        all_genres.extend(g_list)

genre_counter = Counter(all_genres)

# Merge low-frequency genres into "Others", keep top 10
top_n = 10
sorted_genres = sorted(genre_counter.items(), key=lambda x: x[1], reverse=True)
main_genres = sorted_genres[:top_n]
other_count = sum(count for _, count in sorted_genres[top_n:])
genre_counter_final = dict(main_genres)
if other_count > 0:
    genre_counter_final["Others"] = other_count

fig_pie = px.pie(
    names=list(genre_counter_final.keys()),
    values=list(genre_counter_final.values()),
    title="Genre Distribution (Top 10 + Others)"
)
st.plotly_chart(fig_pie)

# -------------------------------
# Visualization 2: Country Distribution (Bar Chart)
# -------------------------------
st.subheader("Production Country Distribution")

def parse_country_list(x):
    """
    Parse production_countries column into a list of country names.
    """
    if isinstance(x, str):
        try:
            obj = eval(x)
        except:
            return []
    elif isinstance(x, list):
        obj = x
    else:
        return []

    if not isinstance(obj, list):
        return []

    country_names = []
    for item in obj:
        if isinstance(item, dict) and "name" in item:
            country_names.append(item["name"])
        elif isinstance(item, str):
            country_names.append(item)
        else:
            country_names.append(str(item))
    return country_names

df_filtered.loc[:, "countries_list"] = df_filtered["production_countries"].apply(parse_country_list)

# Preview first 5 parsed results
st.write("countries_list (first 5 rows):")
st.write(df_filtered["countries_list"].head())

all_countries = []
for c_list in df_filtered["countries_list"]:
    if isinstance(c_list, list):
        all_countries.extend(c_list)

country_counter = Counter(all_countries)

top_n_country = 10
sorted_countries = sorted(country_counter.items(), key=lambda x: x[1], reverse=True)
main_countries = sorted_countries[:top_n_country]
other_count_country = sum(count for _, count in sorted_countries[top_n_country:])
country_counter_final = dict(main_countries)
if other_count_country > 0:
    country_counter_final["Others"] = other_count_country

df_country = pd.DataFrame(list(country_counter_final.items()), columns=["Country", "Count"])
df_country = df_country.sort_values(by="Country")

fig_bar = px.bar(
    df_country, 
    x="Country", 
    y="Count",
    title="Production Countries (Top 10 + Others)",
    labels={"Count": "Movie Count"}
)
st.plotly_chart(fig_bar)

# -------------------------------
# Visualization 3: Score vs. Success Impact (Scatter Plot)
# -------------------------------
st.subheader("Movie Score vs. Success Impact")

fig_scatter = px.scatter(
    df_filtered, 
    x="vote_average", 
    y="success_impact",
    color="original_language", 
    hover_data=["title"],
    title="Score vs. Success Impact by Language",
    labels={"vote_average": "Score", "success_impact": "Impact"}
)
st.plotly_chart(fig_scatter)

# -------------------------------
# Visualization 4: Yearly Movie Count (Line Chart)
# -------------------------------
st.subheader("Movie Release Trends by Year")

year_counts = df_filtered["release_date"].dt.year.value_counts().sort_index()
fig_line = px.line(
    x=year_counts.index, 
    y=year_counts.values,
    title="Movie Release Count by Year",
    labels={"x": "Year", "y": "Count"}
)
st.plotly_chart(fig_line)
