# dzupper-grouper

Simple Map<String,Object> or List<Map> group by parser and expression for Java depending on path-extractor: https://github.com/datazup/path-extractor and dzupper-expression: https://github.com/datazup/dzupper-expression

It requires Redis for processing.

Require grouping definitions: dimensions and metrics

Sample:
```

        String def = "{"
                +"\"dimensions\":[\"$name$\", \"$type$\", \"HOUR($date$)\", \"DAY($date$)\", \"MONTH($date$)\", \"YEAR($date$)\"], "
                +"\"metrics\":[\"SUM($amount$)\", \"COUNT($amount$)\"] "
                +"}";
        // Definition equivalent to SQL: SELECT name, type, hour(date), day(date), month(date), year(date), SUM(amount), COUNT(amount)"

        Map<String,Object> mapDefinition = JsonUtils.getMapFromJson(def);

        List<Map<String,Object>> records = getNestedListOfMaps();
        
        List<String> dimensions = (List<String>) mapDefinition.get("dimensions");
        List<String> metrics = (List<String>) mapDefinition.get("metrics");
        
        String groupName = "group1";
        
        for (Map<String,Object> streamMap: records){
             DimensionKey asyncDimensionKey = new DimensionKey(dimensions, streamMap);
             asyncDimensionKey.build();
             Map<String,Object> currentMap = grouper.upsert(groupName, asyncDimensionKey, metrics);
             System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));
        }
        
        List<Map<String, Object>> groupingList = grouper.getReportList(groupName, dimensions, metrics);
        
        

```