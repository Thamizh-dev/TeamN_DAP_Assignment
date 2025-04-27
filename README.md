# 🕵️‍♂️ Crime Data Analysis in Los Angeles (2013–2024) 

## 📖 Project Overview
This project focuses on **analyzing crime trends** in Los Angeles from **2013 to 2024**.  
We deep dive into crime types, hotspot locations, victim demographics, and arrest patterns to uncover actionable insights.  
Our findings aim to support **crime prevention efforts** and **public safety initiatives**. 🚓

---

## 🎯 Motivation
In a bustling city like **Los Angeles**, crimes have been on a concerning rise due to urban challenges.  
This project was inspired by the need to:
- 📈 Understand year-over-year crime patterns.
- 🧍 Identify vulnerable age and gender groups.
- 🗺️ Map high-risk areas.
- 🚔 Analyze arrest trends.

---

## 🛠️ Tools & Technologies
| Tool | Purpose |
|:----|:--------|
| **Python** | Data analysis & visualization |
| **MongoDB** | Initial data storage (ETL Extraction) |
| **PostgreSQL** | Structured transformed data storage |
| **Docker** | Containerized database environments |
| **Dagster** | ETL workflow orchestration |
| **Matplotlib & Seaborn** | Visual storytelling 📊 |

---

## 📂 Datasets Used
- **Crime Data (2013–2019)** — XML 📄
- **Crime Data (2020–Present)** — CSV 📑
- **Arrest Data (2020–Present)** — XML 📄

🗂️ All datasets are publicly available on [Data.gov](https://www.data.gov/).

---

## 🚀 Methodology
1. **Data Extraction**: Load datasets into MongoDB containers.
2. **Data Transformation**: Clean, merge, create new features (year, combined locations).
3. **Data Loading**: Push cleaned data to PostgreSQL.
4. **Analysis & Visualization**: Identify trends using advanced plots.
5. **Clustering**: Apply K-Means to uncover hidden patterns.

---

## 🔥 Key Insights
- 🛡️ **Top Crimes**: 
  - Simple Assault & Petty Theft are consistently the most frequent.
- 🧑‍🤝‍🧑 **Victims**:
  - Ages **20–45** are most affected.
  - **Females** are slightly more victimized than males.
- 📍 **Crime Hotspots**:
  - **77th Street**, **Southwest**, and **North Hollywood** areas.
- 🚨 **Arrests**:
  - Most arrests involve individuals aged **25–42**.
- 📅 **Crime Trends**:
  - Noticeable spike in **vehicle thefts** post-2020.

---

## 📊 Visual Storytelling
> Visualizations played a key role:
- 📈 Time series plots for yearly crime trend analysis.
- 🧮 Heatmaps for year-crime frequency.
- 🎯 Clustering with K-Means for hidden pattern discovery.
- 🎭 Victim demographics via boxplots & countplots.

---

## 📌 Conclusion
By investigating multiple datasets across a decade:
- We discovered crucial **patterns and demographic factors**.
- Proposed recommendations to **enhance public safety**.
- Created insights that can guide **LAPD** and **city planners** towards more informed strategies.

---

## 🔮 Future Enhancements
- 📡 Real-time crime tracking with streaming data.
- 🤖 Build predictive crime models using Machine Learning.
- 🗺️ Spatial analysis with GIS integration.

---

## 👨‍💻 Team Members
| Name |
| **Nithish Christopher** 
| **Kiruthika Suresh**
| **Tamil Selvan Giri Moorthy**

---

> *"Through data, we don't just study the past — we prepare for a safer future."* 🌟
