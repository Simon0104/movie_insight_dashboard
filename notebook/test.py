import pandas as pd
import json
import matplotlib.pyplot as plt
from collections import Counter
from pymongo import MongoClient

def import_csv_to_mongodb(movies_file="movies.csv", credits_file="credits.csv", 
                            uri="mongodb://localhost:27017/", db_name="movies_db", collection_name="movies"):
    # 读取 CSV 数据
    movies_df = pd.read_csv(movies_file)
    credits_df = pd.read_csv(credits_file)
    # 合并两个数据集（内连接）
    df = pd.merge(movies_df, credits_df, on="id", how="inner")
    
    # 可选：如果某些字段（如 genres, production_countries）需要转换为 JSON 对象，
    # 这里可以进行处理。例如：将它们转换为 Python 列表：
    def fix_json_column(x):
        try:
            return json.loads(x.replace("'", '"'))
        except:
            return []
    
    df["genres"] = df["genres"].apply(fix_json_column)
    df["production_countries"] = df["production_countries"].apply(fix_json_column)
    
    # 连接 MongoDB 并插入数据
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # 将 DataFrame 转换为字典列表，并插入到 MongoDB
    data = df.to_dict(orient="records")
    collection.insert_many(data)
    print(f"✅ 已将 {len(data)} 条数据导入 MongoDB 的 {db_name}.{collection_name} 集合！")
    
# 调用示例
import_csv_to_mongodb()

def question_1(movies_path, credits_path):
    movies_df = pd.read_csv(movies_path)
    credits_df = pd.read_csv(credits_path)
    df1 = pd.merge(movies_df, credits_df, on="id", how="inner")
    print("QUESTION 1: DataFrame shape after join:", df1.shape)
    return df1

def question_2(df1):
    selected_columns = [
        "id", "title", "popularity", "cast", "crew", "budget", "genres",
        "original_language", "production_companies", "production_countries",
        "release_date", "revenue", "runtime", "spoken_languages",
        "vote_average", "vote_count"
    ]
    df2 = df1[selected_columns]
    print("QUESTION 2: DataFrame shape after selecting columns:", df2.shape)
    return df2

def question_3(df2):
    df3 = df2.set_index("id")
    print("QUESTION 3: DataFrame shape after setting index:", df3.shape)
    return df3

def question_4(df3):
    df4 = df3[df3["budget"] > 0]
    print("QUESTION 4: DataFrame shape after dropping budget=0:", df4.shape)

    return df4  # 返回处理后的 DataFrame

def question_5(df4):
    # 1️⃣ 计算 `success_impact`
    df4["success_impact"] = (df4["revenue"] - df4["budget"]) / df4["budget"]

    # 2️⃣ 打印数据形状，确保新列被正确添加
    print("QUESTION 5: DataFrame shape after adding success_impact:", df4.shape)

    return df4  # 返回包含 success_impact 的 DataFrame

def question_6(df5):
    # 1️⃣ 计算最小值和最大值
    min_pop = df5["popularity"].min()
    max_pop = df5["popularity"].max()

    # 2️⃣ 归一化 `popularity` 到 [0, 100]
    df5["popularity"] = 100 * (df5["popularity"] - min_pop) / (max_pop - min_pop)

    # 3️⃣ 打印数据形状，确保新列计算正确
    print("QUESTION 6: DataFrame shape after normalizing popularity:", df5.shape)

    return df5  # 返回归一化后的 DataFrame

def question_7(df6):
    # 1️⃣ 将 `popularity` 转换为 `int16`
    df6["popularity"] = df6["popularity"].astype("int16")

    # 2️⃣ 打印数据形状，确保转换成功
    print("QUESTION 7: DataFrame shape after converting popularity to int16:", df6.shape)

    return df6  # 返回转换后的 DataFrame

def question_8(df7):
    def parse_cast(cast_json):
        try:
            # 1️⃣ 解析 JSON
            cast_list = json.loads(cast_json)  # 解析 JSON 字符串
            # 2️⃣ 提取 name，并按字母顺序排序
            names = sorted([person["name"] for person in cast_list])
            # 3️⃣ 连接成字符串
            return ", ".join(names)
        except:
            return ""  # 如果解析失败，返回空字符串


    # 4️⃣ 应用到 `cast` 列
    df7["cast"] = df7["cast"].apply(parse_cast)

    # 5️⃣ 打印数据形状，确保处理成功
    print("QUESTION 8: DataFrame shape after processing cast:", df7.shape)

    return df7  # 返回处理后的 DataFrame



def question_9(df8):
# 1️⃣ 计算每部电影的角色数量
    df8["num_characters"] = df8["cast"].apply(lambda x: len(x.split(", ")) if x else 0)

    # 2️⃣ 取角色最多的前 10 部电影
    top_10_df = df8.nlargest(10, "num_characters")

    # 3️⃣ 获取电影名称列表
    top_10_movies = top_10_df["title"].tolist()

    # 4️⃣ 打印结果，确保正确
    print("QUESTION 9: Top 10 movies with most characters:", top_10_movies)

    return top_10_movies  # 返回电影名称列表

def question_10(df8):
    # 1️⃣ 确保 `release_date` 是 datetime 格式
    df8["release_date"] = pd.to_datetime(df8["release_date"], errors="coerce")

    # 2️⃣ 按 `release_date` 降序排序
    df10 = df8.sort_values(by="release_date", ascending=False)

    # 3️⃣ 打印数据形状，确保正确排序
    print("QUESTION 10: DataFrame shape after sorting by release_date:", df10.shape)

    return df10  # 返回排序后的 DataFrame

def fix_and_parse_json(json_str):
    """
    尝试将给定的字符串解析为 JSON 对象。
    如果解析失败，则将字符串中的单引号替换为双引号后再解析。
    返回解析后的对象；如果都失败则返回 None。
    """
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        try:
            fixed_str = json_str.replace("'", '"')
            return json.loads(fixed_str)
        except Exception as e:
            print("Error in fix_and_parse_json:", e)
            return None


def question_11(df10, top_n=15):
    """
    - 先将 genres 列填充空值，并确保它是 JSON 字符串格式。
    - 使用 fix_and_parse_json() 解析后提取 'name' 字段，得到各电影类型。
    - 统计所有电影的类型出现次数，合并小类为 'Others'（只保留前 top_n 个）。
    - 绘制饼图，调节标签与百分比位置，并增大 figsize。
    - 返回合并后（含 Others)的类型计数字典。
    """

    # 1. 确保 genres 列不为空
    df10["genres"] = df10["genres"].fillna("[]")
    
    # 2. 如果 genres 是列表类型，则转换成 JSON 字符串
    def ensure_json_format(x):
        if isinstance(x, list):
            return json.dumps(x)
        return x
    df10["genres"] = df10["genres"].apply(ensure_json_format)
    
    # 3. 解析 JSON 并提取 name
    def parse_genres(genres_json):
        if isinstance(genres_json, str) and genres_json.startswith("["):
            parsed = fix_and_parse_json(genres_json)
            if parsed is not None:
                return [g["name"] for g in parsed]
        return []
    
    df10["genres"] = df10["genres"].apply(parse_genres)
    
    # 4. 打印检查解析后的结果（前 10 行）
    print("Parsed genres sample:")
    print(df10["genres"].head(10))
    
    # 5. 统计所有电影中各 genre 出现的频率
    genre_counts = Counter([genre for genres in df10["genres"] for genre in genres])
    
    if not genre_counts:
        print("Error: No genre data found. Pie chart cannot be plotted.")
        return
    
    # ========== 合并小类为 "Others" ==========

    # 将字典转为 (genre, count) 列表，并按出现次数降序排序
    sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)

    # 取前 top_n 个
    main_genres = sorted_genres[:top_n]
    other_genres = sorted_genres[top_n:]
    other_count = sum(count for _, count in other_genres)

    # 生成新的字典：保留前 top_n + 1 个 "Others"
    new_genre_counts = dict(main_genres)
    if other_count > 0:
        new_genre_counts["Others"] = other_count

    # 6. 绘制饼图
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.pie(
        new_genre_counts.values(),
        labels=new_genre_counts.keys(),
        autopct='%1.1f%%',
        startangle=140,
        labeldistance=1.1,   # 标签与圆心的距离
        pctdistance=0.8      # 百分比与圆心的距离
    )
    ax.set_title("Distribution of Movie Genres (Merged Others)")
    plt.show()
    
    return new_genre_counts

def question_12(df10, top_n=13):
    """
    - 填充 production_countries 的空值为 "[]"
    - 调用 fix_and_parse_json() 解析字符串为 Python 对象
    - 提取 'name' 字段统计电影数量
    - 只保留前 top_n 个国家，其余合并为 "Others"
    - 绘制柱状图
    """
    # 1. 确保 production_countries 列不为空
    df10["production_countries"] = df10["production_countries"].fillna("[]")
    
    # 2. 定义解析函数 parse_countries
    def parse_countries(countries_str):
        if not isinstance(countries_str, str):
            return []
        if not countries_str.startswith("["):
            # 如果不是以 "[" 开头，说明不是列表形式，直接返回空
            return []
        
        # 使用 fix_and_parse_json() 尝试解析
        parsed = fix_and_parse_json(countries_str)
        if parsed is not None:
            # parsed 应该是一个 list[dict]，我们提取 "name"
            try:
                return [c["name"] for c in parsed]
            except:
                pass
        return []  # 解析失败或非预期格式
    
    # 应用解析函数到 production_countries 列
    df10["production_countries"] = df10["production_countries"].apply(parse_countries)
    
    # 3. 统计每个国家出现的电影数量
    country_counter = Counter(
        country
        for countries in df10["production_countries"]
        for country in countries
    )
    
    # 如果解析结果为空，就直接返回
    if not country_counter:
        print("No country data parsed.")
        return {}
    
    # 4. 按出现次数从大到小排序
    sorted_countries = sorted(country_counter.items(), key=lambda x: x[1], reverse=True)
    
    # 5. 取前 top_n 个国家，其余合并到 "Others"
    main_countries = sorted_countries[:top_n]
    other_countries = sorted_countries[top_n:]
    other_count = sum(count for _, count in other_countries)
    
    # 生成新的统计字典
    new_country_counts = dict(main_countries)
    if other_count > 0:
        new_country_counts["Others"] = other_count
    
    # 6. 按国家名称字母顺序排序
    new_country_counts = dict(sorted(new_country_counts.items(), key=lambda x: x[0]))
    
    # 7. 绘制柱状图
    plt.figure(figsize=(12, 6))
    plt.bar(new_country_counts.keys(), new_country_counts.values())
    plt.xlabel("Country")
    plt.ylabel("Number of Movies")
    plt.title(f"Number of Movies by Production Country (Top {top_n} + Others)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()
    
    return new_country_counts




# **主程序执行**
if __name__ == "__main__":
    df1 = question_1("movies.csv", "credits.csv")  # 先执行 Question 1
    df1.to_csv("output_df1.csv", index=False)
    print("✅ Saved df1 to output_df1.csv (merged dataset)")

    df2 = question_2(df1)  # 选择指定列
    df2.to_csv("output_df2.csv", index=False)
    print("✅ Saved df2 to output_df2.csv (filtered dataset)")

    df3 = question_3(df2)  # 设定索引
    df3.to_csv("output_df3.csv")
    print("✅ Saved df3 to output_df3.csv (indexed dataset)")

    df4 = question_4(df3)  # 删除 budget=0 的行
    df4.to_csv("output_df4.csv")  # **保存结果**
    print("✅ Saved df4 to output_df4.csv (removed budget=0)")

    df5 = question_5(df4)  # 计算 success_impact
    df5.to_csv("output_df5.csv")  # **保存结果**
    print("✅ Saved df5 to output_df5.csv (added success_impact)")

    df6 = question_6(df5)  # 归一化 popularity
    df6.to_csv("output_df6.csv")  # **保存结果**
    print("✅ Saved df6 to output_df6.csv (normalized popularity)")

    df7 = question_7(df6)
    df7.to_csv("output_df7.csv")
    print("✅ Saved df7 to output_df7.csv (converted popularity to int16)")

    df8 = question_8(df7)  # 解析 cast JSON 并格式化
    df8.to_csv("output_df8.csv")  # **保存结果**
    print("✅ Saved df8 to output_df8.csv (processed cast column)")

    top_10_movies = question_9(df8)  # 找出角色最多的前 10 部电影
    print("✅ Top 10 movies:", top_10_movies)

    df10 = question_10(df8)  # 按 release_date 降序排序
    df10.to_csv("output_df10.csv")  # **保存结果**
    print("✅ Saved df10 to output_df10.csv (sorted by release_date)")

    genre_counts = question_11(df10)  # 绘制 `genres` 饼图
    print("✅ Genre distribution:", genre_counts)

    country_counts = question_12(df10)
    print("✅ Production countries distribution:", country_counts)

