### 1st: process_data.py
- cleaning and processing data
- sort by division_sort & person_sort
- return clean json files

```mermaid
flowchart TD
    A[Start] --> B[Load JSON Data]
    
    %% Cleaning pipeline with dashed line
    subgraph "Cleaning pipeline"
    C[validate required keys]
    D[strip spaces]
    E[map org_id to org_sort]
    F[validate org_type]
    G[validate person_email]
    H[validate person_phone]
    I[capitalize values]
    J[standardize_position_sort_key]
    end

    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J

    J --> K{Sort by division_sort & person_sort}
    K -- Reset person_sort by division --> L[Sort by Division]
    K -- Do not reset person_sort by each division --> M[Sort by Organisation]
    L --> N[remove_keys & reorder_keys]
    M --> N[remove_keys & reorder_keys]
    N --> O[write to output folder]
```
### 2nd: compiling script
- compile the cleaned data
- sort by org_id, division_sort, person_sort
- return compiled data
```mermaid
flowchart TD
    G1 --> A
    A[Start] --> B[compile JSON files]
    B --> C[sort data by org_id]
    C --> D[return output file]
    %% This part connects the previous flowchart to the new one
    subgraph Previous Pipeline Output
        G1[Processed Data]
    end
```
