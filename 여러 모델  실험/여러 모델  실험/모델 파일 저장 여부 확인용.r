# Databricks notebook source
### File 저장 잘 되었는 지 확인 
load(file='/dbfs/FileStore/model/gamma_glm.RData')

# COMMAND ----------

gamma.glm

# COMMAND ----------

ls()

# COMMAND ----------

# Load the data, and store the name of the loaded object in x
x = load('data.Rsave')
# Get the object by its name
y = get(x)
# Remove the old object since you've stored it in y 
rm(x)