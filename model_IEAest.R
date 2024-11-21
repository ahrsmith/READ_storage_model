library(dplyr)
library(data.table)
library(purrr)

Sys.setenv(TZ='Europe/Paris')

print_timestamped = function(message) {
  timestamped_message = paste(Sys.time(), message)
  cat(timestamped_message)
  flush.console()
  Sys.sleep(1)
}

schedules = expand.grid(replicate(24, c(0, 1), simplify = FALSE))

#test case 
feed_rate = 12 #tons/day

#technical assumptions
biogas_yield = 75000 #L biogas per ton feedstock 
biogas_per_day = feed_rate*biogas_yield #L per day
energy_density_IEA = 0.222 #toe per tonne feedstock
conv_toe_to_MWh = 11.63 #MWh per toe
conv_kW_to_MW=1/1000 #MWh per kWh
conv_MW_to_kW=1000 #kWh to MWh
#conv_MJ_to_kWh = 1 / 3.6 #MJ per m^3 to kWh per m^3
#conv_m3_to_L = 1/1000 #kWh per m^3 to kWh per L
energy_density = energy_density_IEA*conv_toe_to_MWh*conv_MW_to_kW #final kWh per tonne feedstock
turbine_eff = .4
storage_lifetime = 20 #years 
gen_lifetime = 20 #years
winter_Atable_summary = NULL
spring_Atable_summary = NULL
summer_Atable_summary = NULL
fall_Atable_summary = NULL
winter_Btable_summary = NULL
spring_Btable_summary = NULL
summer_Btable_summary = NULL
fall_Btable_summary = NULL
size_gen_dict = NULL
#load elect prices
elect_prices_win = read.csv("20231220_20231222_PRC_LMP_DAM_20241016_19_51_18_v12.csv") ##using dec 21st, 2023...node = DAVIS_0_N030
elect_prices_spr = read.csv("20240321_20240323_PRC_LMP_DAM_20241016_19_50_28_v12.csv") ##using mar 22nd, 2024...node = DAVIS_0_N030
elect_prices_sum = read.csv("20240619_20240621_PRC_LMP_DAM_20241016_19_49_58_v12.csv") ##using June 20th, 2024...node = DAVIS_0_N030
elect_prices_fal = read.csv("20240921_20240923_PRC_LMP_DAM_20241016_19_49_24_v12.csv") ##using sep 22nd, 2024...node = DAVIS_0_N030

elect_prices_win = elect_prices_win[elect_prices_win$XML_DATA_ITEM=="LMP_ENE_PRC",]
elect_prices_win = elect_prices_win[order(elect_prices_win$INTERVALSTARTTIME_GMT),]
elect_prices_win = elect_prices_win[25:48,]
elect_prices_win = t(elect_prices_win$MW) # electricity prices are per MWh
elect_prices_win=as.data.table(elect_prices_win)

elect_prices_spr = elect_prices_spr[elect_prices_spr$XML_DATA_ITEM=="LMP_ENE_PRC",]
elect_prices_spr = elect_prices_spr[order(elect_prices_spr$INTERVALSTARTTIME_GMT),]
elect_prices_spr = elect_prices_spr[25:48,]
elect_prices_spr = t(elect_prices_spr$MW) # electricity prices are per MWh
elect_prices_spr=as.data.table(elect_prices_spr)

elect_prices_sum = elect_prices_sum[elect_prices_sum$XML_DATA_ITEM=="LMP_ENE_PRC",]
elect_prices_sum = elect_prices_sum[order(elect_prices_sum$INTERVALSTARTTIME_GMT),]
elect_prices_sum = elect_prices_sum[25:48,]
elect_prices_sum = t(elect_prices_sum$MW) # electricity prices are per MWh
elect_prices_sum=as.data.table(elect_prices_sum)

elect_prices_fal = elect_prices_fal[elect_prices_fal$XML_DATA_ITEM=="LMP_ENE_PRC",]
elect_prices_fal = elect_prices_fal[order(elect_prices_fal$INTERVALSTARTTIME_GMT),]
elect_prices_fal = elect_prices_fal[25:48,]
elect_prices_fal = t(elect_prices_fal$MW) # electricity prices are per MWh
elect_prices_fal = as.data.table(elect_prices_fal)

for (gen_hours in 1:24) {
  schedules_live = schedules[rowSums(schedules) == gen_hours, ]
  double_sched = cbind(schedules_live,schedules_live)
  colnames(double_sched) = -24:23
  
  cumulative_sum = function(row) {
    result <- accumulate(row, function(accum, x) {
      if (x == 1 && (24 / gen_hours) > accum) {
        accum = 0 
      } else if (x == 1) {
        accum = accum - (24 / gen_hours) 
      } else if (x == 0) {
        accum = accum + 1  
      }
      accum
    }, .init = 0)
    
    return(max(result))
  }
  
  max_values = double_sched %>%
    rowwise() %>%
    mutate(max_storage_capacity = cumulative_sum(c_across(everything()))) %>%
    ungroup()
  
  max_values$max_storage_capacity = max_values$max_storage_capacity*biogas_per_day/24
  max_values$Generator_Hours = gen_hours
  
  storage_maintain_cost = 0
  storage_capital = 0.8*max_values$max_storage_capacity*5 ###FINALIZE
  total_storage_cost = storage_capital + storage_maintain_cost 
  storage_cost_per_day = total_storage_cost / (storage_lifetime*365)
  
  min_size_generator = feed_rate * energy_density * turbine_eff / gen_hours ##biogas in L; energy density in kWh per L; gen_hours in hr; final in kW
  size_gen = ceiling(min_size_generator)
  
  gen_maintain_cost = 0
  gen_capital_cost = 200*size_gen*10 ###FINALIZE
  total_gen_cost = gen_maintain_cost + gen_capital_cost
  gen_cost_per_day = total_gen_cost / (gen_lifetime*365)
  
  elect_sched = double_sched[,25:48]*size_gen/1000 ##divide by 1000 to convert from kWh to MWh
  elect_sched=as.data.table(elect_sched)
  revenue_schedule_win = elect_sched*elect_prices_win[rep(1,nrow(elect_sched)),]
  revenue_schedule_win$Daily_rev_win = rowSums(revenue_schedule_win)
  revenue_schedule_spr = elect_sched*elect_prices_spr[rep(1,nrow(elect_sched)),]
  revenue_schedule_spr$Daily_rev_spr = rowSums(revenue_schedule_spr)
  revenue_schedule_sum = elect_sched*elect_prices_sum[rep(1,nrow(elect_sched)),]
  revenue_schedule_sum$Daily_rev_sum = rowSums(revenue_schedule_sum)
  revenue_schedule_fal = elect_sched*elect_prices_fal[rep(1,nrow(elect_sched)),]
  revenue_schedule_fal$Daily_rev_fal = rowSums(revenue_schedule_fal)
  
  ##B tables
  win_sched_summary = cbind(max_values,storage_cost_per_day,gen_cost_per_day,revenue_schedule_win$Daily_rev)
  spr_sched_summary = cbind(max_values,storage_cost_per_day,gen_cost_per_day,revenue_schedule_spr$Daily_rev)
  sum_sched_summary = cbind(max_values,storage_cost_per_day,gen_cost_per_day,revenue_schedule_sum$Daily_rev)
  fal_sched_summary = cbind(max_values,storage_cost_per_day,gen_cost_per_day,revenue_schedule_fal$Daily_rev)
  
  win_sched_summary$profit = win_sched_summary$`revenue_schedule_win$Daily_rev`-win_sched_summary$storage_cost-win_sched_summary$gen_cost
  spr_sched_summary$profit = spr_sched_summary$`revenue_schedule_spr$Daily_rev`-spr_sched_summary$storage_cost-spr_sched_summary$gen_cost
  sum_sched_summary$profit = sum_sched_summary$`revenue_schedule_sum$Daily_rev`-sum_sched_summary$storage_cost-sum_sched_summary$gen_cost
  fal_sched_summary$profit = fal_sched_summary$`revenue_schedule_fal$Daily_rev`-fal_sched_summary$storage_cost-fal_sched_summary$gen_cost
  
  if (is.null(winter_Btable_summary)) {
    winter_Btable_summary = win_sched_summary
  } else {
    winter_Btable_summary = rbind (winter_Btable_summary,win_sched_summary)
  }
  
  if (is.null(spring_Btable_summary)) {
    spring_Btable_summary = spr_sched_summary
  } else {
    spring_Btable_summary = rbind (spring_Btable_summary,spr_sched_summary)
  }
  
  if (is.null(summer_Btable_summary)) {
    summer_Btable_summary = sum_sched_summary
  } else {
    summer_Btable_summary = rbind (summer_Btable_summary,sum_sched_summary)
  }
  
  if (is.null(fall_Btable_summary)) {
    fall_Btable_summary = fal_sched_summary
  } else {
    fall_Btable_summary = rbind (fall_Btable_summary,fal_sched_summary)
  }
  
  ##A tables
  win_sched_summary = win_sched_summary %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  spr_sched_summary = spr_sched_summary %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  sum_sched_summary = sum_sched_summary %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  fal_sched_summary = fal_sched_summary %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  
  ##add to interloop
  if (is.null(winter_Atable_summary)) {
    winter_Atable_summary = win_sched_summary
  } else {
    winter_Atable_summary = rbind (winter_Atable_summary,win_sched_summary)
  }
  
  if (is.null(spring_Atable_summary)) {
    spring_Atable_summary = spr_sched_summary
  } else {
    spring_Atable_summary = rbind (spring_Atable_summary,spr_sched_summary)
  }
  
  if (is.null(summer_Atable_summary)) {
    summer_Atable_summary = sum_sched_summary
  } else {
    summer_Atable_summary = rbind (summer_Atable_summary,sum_sched_summary)
  }
  
  if (is.null(fall_Atable_summary)) {
    fall_Atable_summary = fal_sched_summary
  } else {
    fall_Atable_summary = rbind (fall_Atable_summary,fal_sched_summary)
  }
  
  dictionary_sizes = data.table("Constructed Generator Hours"=gen_hours,"Size Generator"=size_gen,"gen_cost"=gen_cost_per_day)
  if (is.null(size_gen_dict)) {
    size_gen_dict = dictionary_sizes
  } else {
    size_gen_dict = rbind (size_gen_dict,dictionary_sizes)
  }
  
  percent_done = nrow(winter_Btable_summary)/(nrow(schedules))*100
  print_timestamped(sprintf("finished processing gen_hours %f. approx %f%% done.",gen_hours,percent_done))
  flush.console()
}

print_timestamped("starting A table loop")
season_bal_summary = NULL

#Atable summary
for (gen_hr in 1:24) { 
  higher_hours_winter = winter_Atable_summary %>% filter(Generator_Hours>=gen_hr)
  higher_hours_winter = higher_hours_winter %>% select(max_storage_capacity,storage_cost_per_day,Generator_Hours,profit,`revenue_schedule_win$Daily_rev`)
  higher_hours_spring = spring_Atable_summary %>% filter(Generator_Hours>=gen_hr)
  higher_hours_spring = higher_hours_spring %>% select(max_storage_capacity,profit,`revenue_schedule_spr$Daily_rev`)
  higher_hours_summer = summer_Atable_summary %>% filter(Generator_Hours>=gen_hr)
  higher_hours_summer = higher_hours_summer %>% select(max_storage_capacity,profit,`revenue_schedule_sum$Daily_rev`)
  higher_hours_fall = fall_Atable_summary %>% filter(Generator_Hours>=gen_hr)
  higher_hours_fall = higher_hours_fall %>% select(max_storage_capacity,profit,`revenue_schedule_fal$Daily_rev`)
  
  higher_hours_winter = higher_hours_winter %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  higher_hours_spring = higher_hours_spring %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  higher_hours_summer = higher_hours_summer %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  higher_hours_fall = higher_hours_fall %>% 
    group_by(max_storage_capacity) %>% 
    filter(profit == max(profit)) %>% 
    ungroup()
  
  higher_hours_merge_profits = higher_hours_winter %>%
    left_join(higher_hours_spring, by="max_storage_capacity") %>%
    left_join(higher_hours_summer, by="max_storage_capacity") %>%
    left_join(higher_hours_fall, by="max_storage_capacity")
  
  higher_hours_merge_profits = higher_hours_merge_profits %>%
    mutate(Season_Sum_Revenue = `revenue_schedule_win$Daily_rev` + `revenue_schedule_spr$Daily_rev` + `revenue_schedule_sum$Daily_rev` + `revenue_schedule_fal$Daily_rev`)
  
  higher_hours_merge_profits$"Constructed Generator Hours" = gen_hr

  
  higher_hours_merge_profits = higher_hours_merge_profits %>% select(-profit.x,-profit.y,-profit.x.x,-profit.y.y)

  best_profit_season_balance = higher_hours_merge_profits %>%
    filter(Season_Sum_Revenue == max (Season_Sum_Revenue))
    
  colnames(best_profit_season_balance) = c("max_storage","stor_cost","Actual Gen Hours","rev_win","rev_spr","rev_sum","rev_fal","Season_Sum_Revenue","Constructed Generator Hours")
  best_profit_season_balance = merge(best_profit_season_balance,size_gen_dict,by.x="Constructed Generator Hours",all.x=TRUE)  
  
  
  best_profit_season_balance = best_profit_season_balance %>%
    mutate(Season_Bal_Profit = Season_Sum_Revenue - gen_cost*4 - stor_cost*4)
  
  ##add to interloop
  if (is.null(season_bal_summary)) {
    season_bal_summary = best_profit_season_balance
  } else {
    season_bal_summary = rbind (season_bal_summary,best_profit_season_balance)
  }
  
  
  percent_done = gen_hr/24*100
  
  print_timestamped(sprintf("finished processing gen_hours %f. ROUGHLY ROUGHLY %f%% done.",gen_hr,percent_done))
}

print_timestamped("merging and ordering final A tables")
#tag the generator size onto balanced season

Final_Balanced = season_bal_summary[order(season_bal_summary$Season_Bal_Profit,decreasing = TRUE),]
print_timestamped("final A tables complete")

print_timestamped("ordering final B tables")
#Btable summaries
winter_Btable_summary = winter_Btable_summary[order(winter_Btable_summary$profit,decreasing = TRUE),]

spring_Btable_summary = spring_Btable_summary[order(spring_Btable_summary$profit,decreasing = TRUE),]

summer_Btable_summary = summer_Btable_summary[order(summer_Btable_summary$profit,decreasing = TRUE),]

fall_Btable_summary = fall_Btable_summary[order(fall_Btable_summary$profit,decreasing = TRUE),]

print_timestamped("writing files")
Final_Winter = head(winter_Btable_summary[, c(25:ncol(winter_Btable_summary))],100)
Final_Spring = head(spring_Btable_summary[, c(25:ncol(spring_Btable_summary))],100)
Final_Summer = head(summer_Btable_summary[, c(25:ncol(summer_Btable_summary))],100)
Final_Fall = head(fall_Btable_summary[, c(25:ncol(fall_Btable_summary))],100)

fwrite(Final_Balanced,"Bal_Summ_exp.csv")
fwrite(Final_Winter,"Win_Summ_exp.csv")
fwrite(Final_Spring,"Spr_Summ_exp.csv")
fwrite(Final_Summer,"Sum_Summ_exp.csv")
fwrite(Final_Fall,"Fal_Summ_exp.csv")

print_timestamped("script completed")