# Business Insights & Analytics Summary

**Project:** Brazilian E-Commerce Data Warehouse & ETL Pipeline  
**Dataset:** Olist Brazilian E-Commerce (2016-2018)  
**Total Revenue Analyzed:** R$ 15,876,790.88  
**Analysis Period:** September 2016 - September 2018

---

## 📊 Executive Summary

### Key Performance Indicators

| Metric | Value | Insight |
|--------|-------|---------|
| **Total Revenue** | R$ 15,876,790.88 | ~R$16M in 2 years |
| **Total Orders** | 98,666 | Strong customer base |
| **Unique Customers** | 98,666 | 1 order per customer avg |
| **Average Order Value** | R$ 140.55 | Mid-range pricing |
| **Average Delivery Time** | 12.0 days | Industry competitive |
| **On-Time Delivery Rate** | 92.10% | Excellent logistics |
| **Average Review Score** | 4.04/5 | High satisfaction |

### Critical Finding
⚠️ **Customer Retention Issue:** Average of 1 order per customer indicates low repeat purchase rate. This is the #1 priority for business growth.

---

## 👥 Customer Analytics

### 1. RFM Segmentation Results

| Segment | Customers | Avg Revenue/Customer | Total Revenue | % of Revenue |
|---------|-----------|---------------------|---------------|--------------|
| **At Risk** 🔴 | 23,194 | R$ 237 | R$ 5,502,348 | **34.7%** |
| **Champions** 🏆 | 16,267 | R$ 297 | R$ 4,833,450 | **30.5%** |
| **Loyal Customers** ✅ | 19,738 | R$ 172 | R$ 3,400,960 | 21.4% |
| **Lost Customers** ❌ | 16,273 | R$ 55 | R$ 892,010 | 5.6% |
| **New Customers** 🌱 | 15,224 | R$ 54 | R$ 820,564 | 5.2% |

**Key Insights:**
- 🚨 **"At Risk" customers are the largest segment (34.7% of revenue)** - immediate retention campaign needed
- 🏆 Champions have highest value (R$297 avg) but represent only 30.5% of revenue
- ❌ Lost + New customers represent only 10.8% of revenue - need better onboarding

**Recommended Actions:**
1. Launch retention campaign for "At Risk" segment (23,194 customers)
2. Create VIP program for Champions (16,267 customers)
3. Implement win-back offers for Lost customers
4. Improve first-purchase experience to convert New → Loyal

---

### 2. Customer Lifetime Value (CLV)

**Top Customer Insights:**
- **Highest CLV:** R$ 13,664 (Rio de Janeiro customer, 8 items in single order)
- **Average Tenure:** 0 days for top 20 customers (all single-order purchases!)
- **Geographic Concentration:** São Paulo dominates top customer locations

**Critical Finding:**
> High-value customers are **one-time big spenders**, not repeat buyers. The business model currently relies on customer acquisition rather than retention.

**Opportunity:** Converting top 20% of customers into repeat buyers could increase revenue by 40-60% based on industry standards.

---

### 3. Customer Acquisition Trends

| Period | New Customers/Month | Growth Pattern |
|--------|---------------------|----------------|
| **Late 2016** | 3-308 | Startup phase |
| **Early 2017** | 789-3,660 | Rapid growth (3-5x MoM) |
| **Nov 2017** | **7,451** | 🚀 **Black Friday spike** |
| **2018** | 6,000-7,200 | Stable growth |

**Key Insights:**
- Black Friday 2017 drove biggest acquisition (7,451 customers)
- 2018 shows mature, consistent growth (~6,500/month)
- Total: 98,666 customers acquired over 2 years

---

## 💰 Revenue Analytics

### 1. Monthly Revenue Trends

**Best Performing Months:**
- 🥇 **November 2017:** R$ 1,182,653 (+53.53% growth) - Black Friday
- 🥈 **March 2018:** R$ 1,156,973 (+16.70%)
- 🥉 **April 2018:** R$ 1,159,728 (+0.24%)

**Seasonal Patterns:**
- Q4 surge every year (October-December)
- Post-holiday dips in January-February
- Mid-year slowdown in June
- Revenue plateaued at ~R$1M/month in 2018

**Growth Story:**
- Started: R$354 (Sep 2016)
- Peak: R$1,182,653 (Nov 2017)
- Stable: ~R$1M/month (2018)

---

### 2. Day of Week Patterns

| Day | Orders | Revenue | Strategy |
|-----|--------|---------|----------|
| **Monday** 👑 | 16,068 | R$ 2,603,933 | Best day for launches |
| Tuesday | 15,831 | R$ 2,537,450 | Strong follow-up |
| Wednesday | 15,425 | R$ 2,471,538 | Mid-week steady |
| Friday | 14,002 | R$ 2,286,863 | Highest AOV (R$142) |
| **Saturday** 📉 | 10,813 | R$ 1,756,100 | Lowest performance |

**Insights:**
- Weekdays generate 77% of revenue
- Monday has 48% more orders than Saturday
- Friday customers spend 4% more (weekend mindset)

**Actions:**
- Launch promotions Monday mornings
- Weekend flash sales to boost slow days
- Email campaigns Sunday evening

---

## 🛍️ Product Analytics

### 1. Top Revenue Categories

| Rank | Category | Revenue | Avg Price | Rating | Market Share |
|------|----------|---------|-----------|--------|--------------|
| 🥇 | Health & Beauty | R$ 1,444,254 | R$ 130 | ⭐ 4.14 | 8.59% |
| 🥈 | Watches & Gifts | R$ 1,305,934 | R$ 201 | ⭐ 4.02 | 5.31% |
| 🥉 | Bed/Bath/Table | R$ 1,248,801 | R$ 93 | ⭐ 3.90 | 9.90% |
| 4 | Sports & Leisure | R$ 1,160,003 | R$ 114 | ⭐ 4.11 | 7.67% |
| 5 | Computer Accessories | R$ 1,062,725 | R$ 116 | ⭐ 3.94 | 6.95% |

**Key Insights:**
- 💄 Health & Beauty: Best overall (highest revenue + rating)
- 💎 Watches/Gifts: Premium segment (highest avg price R$201)
- 📦 Bed/Bath/Table: Volume leader (11,180 items)
- ⚠️ Bed/Bath/Table has lowest rating (3.90) - quality concerns

---

### 2. Seasonal Category Trends

**Q4 (Holiday Season):**
- 🎁 Watches & Gifts surge to #1 (Christmas gifting)
- 🧸 Toys enter top 5
- 💻 Electronics increase

**Q2 (Mother's Day):**
- 💄 Health & Beauty dominates
- 🏠 Housewares increases

**Q1 (New Year):**
- 💻 Computer Accessories peaks
- 🏋️ Sports & Leisure strong

---

### 3. Individual Product Performance

**Top Product:** Health & Beauty item (R$ 67,944 revenue, 4.22 rating)

**Premium Products:**
- 🎸 Musical Instruments: R$ 1,926 avg price (only 13 units sold - huge untapped market!)
- 💻 Computers: R$ 1,397 avg price (4.57 rating - excellent quality)

**Warning Products:**
- 🚗 Auto product: 1.18 rating (urgent quality issue!)
- 👶 Baby product: 2.68 rating (customer safety concern)

---

## 🚚 Operational Analytics

### 1. Delivery Performance by State

**Best Performers:**
| State | Orders | Avg Days | On-Time % |
|-------|--------|----------|-----------|
| 🥇 RO | 243 | 19.3 | 95.97% |
| 🥈 AM | 145 | 25.9 | 95.73% |
| 🥉 PR | 4,923 | 11.5 | 95.21% |
| SP | 40,495 | **8.3** | 94.24% |

**Worst Performers:**
| State | Orders | Avg Days | On-Time % |
|-------|--------|----------|-----------|
| AL | 397 | 24.0 | 75.99% ❌ |
| MA | 717 | 21.2 | 79.68% ❌ |
| SE | 335 | 21.0 | 83.73% ⚠️ |

**Key Insights:**
- 🏙️ São Paulo advantage: Fastest delivery (8.3 days) + massive volume (40,495 orders)
- 🗺️ Northeast struggles: AL, MA, SE all below 85% on-time
- ⚡ SP vs RJ gap: SP delivers in 8.3 days vs RJ's 14.7 days

**Actions:**
- Open fulfillment centers in Northeast Brazil
- Improve logistics partnerships in AL, MA, SE
- Use SP/PR logistics model for other states

---

### 2. Review Score & Delivery Correlation

| Rating | Orders | Avg Delivery Days | % of Orders |
|--------|--------|-------------------|-------------|
| ⭐⭐⭐⭐⭐ | 63,067 | **10.2 days** | 55.83% |
| ⭐⭐⭐⭐ | 21,160 | 11.7 days | 18.73% |
| ⭐⭐⭐ | 9,335 | 13.6 days | 8.26% |
| ⭐⭐ | 3,825 | 15.3 days | 3.39% |
| ⭐ | 14,060 | **19.1 days** | 12.45% |

**Critical Finding:**
> **Delivery time directly impacts rating!** 5-star reviews have 10.2 day delivery vs 19.1 days for 1-star reviews. Almost DOUBLE the time!

**ROI Calculation:**
- Reducing delivery by 3 days could move 20% of 3-star → 4-star
- Potential revenue impact: +R$1.5M annually

---

### 3. Top Seller Performance

**#1 Seller:** Guariba, SP (R$ 249,640 revenue, 4.12 rating)

**Geographic Distribution:**
- 75% of top 20 sellers are in São Paulo
- Only 5 sellers outside SP in top 20
- Opportunity: Recruit sellers in underserved states

**Quality Leaders:**
- Teresópolis, RJ: 4.43 rating (highest!)
- Sumaré, SP: 4.34 rating + R$331 avg price

**Concerns:**
- Itaquaquecetuba, SP: 3.35 rating despite R$240K revenue (volume over quality)

---

## 💳 Payment Analytics

| Payment Method | Market Share | Avg Payment | Avg Installments |
|----------------|--------------|-------------|------------------|
| 💳 **Credit Card** | **75.63%** | R$ 183 | 3.7x |
| 📄 Boleto | 20.30% | R$ 178 | 1.0x |
| 🎫 Voucher | 2.58% | R$ 134 | 2.6x |
| 💳 Debit Card | 1.50% | R$ 150 | 1.0x |

**Key Insights:**
- Credit card dominance (75.63%) reflects Brazilian installment culture
- Average 3.7 installments shows affordability is critical
- Boleto serves 20% of market (unbanked/no credit population)
- Voucher usage low (2.58%) - opportunity for loyalty program

**Brazilian Market Insight:**
> Installment payments are NOT a premium feature - they're essential for the Brazilian market. Average customer spreads payment across 3.7 months to afford purchases.

---

## 🎯 Strategic Recommendations

### Priority 1: Fix Customer Retention (Critical)
**Problem:** 1 order per customer average
- **Action:** Launch email re-engagement campaign
- **Target:** "At Risk" segment (23,194 customers, R$5.5M revenue)
- **Expected Impact:** 10% retention = +R$550K revenue

### Priority 2: Improve Northeast Logistics (High)
**Problem:** AL, MA, SE below 85% on-time delivery
- **Action:** Partner with regional logistics providers
- **Action:** Open fulfillment center in Northeast hub (Salvador or Recife)
- **Expected Impact:** 5% rating improvement in Northeast states

### Priority 3: Reduce Delivery Times (High)
**Problem:** Delivery time directly impacts ratings
- **Action:** Target <10 days delivery for all major cities
- **Expected Impact:** Move 10% of 3-star → 4-star reviews

### Priority 4: Seasonal Inventory Planning (Medium)
**Problem:** Category performance varies by quarter
- **Action:** Stock up Watches/Gifts in Q3 for Q4 surge
- **Action:** Health & Beauty inventory for Q2 (Mother's Day)
- **Expected Impact:** Reduce stockouts during peak seasons

### Priority 5: Geographic Expansion (Medium)
**Problem:** 75% of top sellers in São Paulo
- **Action:** Recruit sellers in RJ, MG, BA, PR
- **Action:** Seller onboarding program for new regions
- **Expected Impact:** Better coverage = faster delivery nationwide

### Priority 6: Payment Optimization (Low)
**Problem:** Voucher usage only 2.58%
- **Action:** Launch cashback/loyalty voucher program
- **Action:** Promote installment-free purchases with vouchers
- **Expected Impact:** Increase repeat purchases by 5%

---

## 📈 Growth Opportunities

### 1. Untapped Premium Market
- Musical instruments: Only 13 units sold at R$1,926 avg
- Computer category: R$1,397 avg price with 4.57 rating
- **Opportunity:** Focus on high-margin premium categories

### 2. Weekend Revenue Gap
- Weekends 39% lower than Mondays
- **Opportunity:** Weekend flash sales could add R$500K/year

### 3. Category Expansion
- "Unknown" category has 610 products
- **Opportunity:** Proper categorization improves discoverability

### 4. Geographic Markets
- 40,495 orders from SP vs only 12,353 from RJ
- **Opportunity:** RJ market is underpenetrated

---

## 🎓 Technical Achievements

This analysis demonstrates:
- ✅ Star schema dimensional modeling
- ✅ Advanced SQL (CTEs, window functions, complex joins)
- ✅ RFM segmentation methodology
- ✅ Cohort analysis techniques
- ✅ ETL pipeline design
- ✅ Data quality management (99.95% clean data)
- ✅ Business intelligence & storytelling

**Technologies Used:**
- PostgreSQL 18
- SQL (DDL, DML, DQL)
- Data warehouse design
- Git version control

---

## 📝 Methodology Notes

**Data Quality:**
- Started with 1,548,331 raw records
- Removed 823 invalid records (0.05%)
- Final dataset: 1,547,508 clean records
- Quality score: 99.95%

**Analysis Approach:**
1. Exploratory data analysis
2. Customer segmentation (RFM)
3. Revenue trend analysis
4. Operational metrics
5. Product performance
6. Geographic analysis
7. Correlation studies (delivery vs ratings)

**Limitations:**
- Data covers 2016-2018 (may not reflect current market)
- Single platform (Olist) - not full market view
- Brazilian market only - findings may not generalize

---

## 🏆 Conclusion

This e-commerce platform demonstrates:
- **Strong foundation:** R$15.87M revenue, 92.10% on-time delivery, 4.04 rating
- **Critical weakness:** Customer retention (1 order per customer avg)
- **Key strength:** Logistics excellence in São Paulo
- **Major opportunity:** Fixing retention could increase revenue 40-60%

**Next Steps:**
1. Implement retention campaign (Priority 1)
2. Improve Northeast logistics (Priority 2)
3. Focus on delivery time reduction (Priority 3)

---

*Analysis completed using star schema data warehouse with 112,960 transactions across 5 dimension tables.*

*For technical implementation details, see SQL scripts in `/sql/` directory.*

*Last Updated: February 2026*
