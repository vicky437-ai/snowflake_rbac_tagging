# CORTEX CLI PROMPT: Informatica PowerCenter XML to Snowflake DBT Migration Analysis

## COMPREHENSIVE NOTEBOOK ANALYSIS PROMPT

---

```
You are a Senior Data Engineering Architect with deep expertise in:
- Informatica PowerCenter (mappings, workflows, transformations, XML structure)
- DBT (Data Build Tool) best practices, project structure, and model design
- Snowflake architecture and optimization
- LLM-based code generation and prompt engineering
- Enterprise data migration strategies

I am attaching my Snowflake notebook: **BABU_INFORMATICA_TO_DBT_V2.ipynb**

This notebook is an AI-powered migration tool that converts Informatica PowerCenter production workflows (XML) to DBT projects for Snowflake.

---

## CURRENT STATE CONTEXT

### What We Have
- **9 Informatica PowerCenter production workflows** of various complexity from a prospect
- **Target: 5 DBT projects** to be generated
- **Notebook: 2,063 lines of code** (including 466-line prompt)
- **LLM Model Used**: Claude Sonnet 4

### Current Notebook Architecture
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    INFORMATICA TO DBT MIGRATION PIPELINE                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│   │  Read XML   │───►│  Analyze &  │───►│  Generate   │───►│  Validate   │ │
│   │  from Stage │    │  Chunk XML  │    │  DBT Code   │    │  & Write    │ │
│   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │
│                                                                             │
│   Components:                                                               │
│   • Config, logging, error tracking setup                                   │
│   • XML parsing (mappings, transformations, instances, connectors)          │
│   • Token calculation and chunking logic                                    │
│   • 466-line prompt for code generation                                     │
│   • Validation of generated DBT code                                        │
│   • Write to Snowflake table → Extract to GIT                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What Works Well (Acknowledged Strengths)
1. ✅ Generates clean, well-structured DBT code
2. ✅ Excellent logging of transformation mapping decisions
3. ✅ Fast execution (~45 seconds per workflow)
4. ✅ Self-validates generated code
5. ✅ Correct DBT project structure output

### Known Limitations (Areas for Improvement)
1. ❌ **Chunking/Merging Issues**: Large XML workflows exceed token limits, causing response truncation
2. ❌ **Static Model Generation**: Always creates 3 models (stage/intermediate/marts) per workflow instead of intelligently determining model type
3. ❌ **Single-Workflow Processing**: Processes one XML at a time instead of understanding the full workflow ecosystem

### Desired Future State
```
CURRENT:  1 XML → 1-3 DBT Models (isolated processing)
DESIRED:  9 XMLs → Holistic Analysis → Optimized DBT Project Structure (5 projects)
```

---

## ANALYSIS REQUIREMENTS

### Task 1: Complete Notebook Analysis
Perform a thorough analysis of the attached notebook covering:

1. **Code Architecture Review**
   - Overall structure and organization
   - Separation of concerns
   - Modularity and reusability
   - Error handling patterns

2. **XML Processing Logic**
   - How Informatica XML elements are parsed
   - Handling of complex transformations (Source Qualifiers, Expressions, Filters, Lookups, Joiners, Aggregators, etc.)
   - Metadata extraction completeness

3. **Chunking Strategy Analysis**
   - Current chunking implementation
   - Token calculation accuracy
   - Chunk boundary handling
   - Context preservation across chunks

4. **Prompt Engineering Review**
   - Quality of the 466-line prompt
   - Instruction clarity and completeness
   - Output format specifications
   - Edge case handling in prompts

5. **DBT Code Generation**
   - Model naming conventions
   - SQL generation quality
   - Transformation logic accuracy
   - Schema handling

6. **Validation Logic**
   - What validations are performed
   - Validation completeness
   - Error reporting quality

7. **Performance & Scalability**
   - Execution efficiency
   - Memory management
   - Scalability for larger workflows

8. **Logging & Observability**
   - Logging comprehensiveness
   - Debug-ability
   - Audit trail quality

---

### Task 2: Scoring Assessment

Provide a detailed scoring matrix:

| Category | Score (1-10) | Weight | Weighted Score | Notes |
|----------|--------------|--------|----------------|-------|
| Code Quality & Structure | /10 | 15% | | |
| XML Parsing Completeness | /10 | 15% | | |
| Chunking Strategy | /10 | 15% | | |
| Prompt Engineering | /10 | 15% | | |
| DBT Output Quality | /10 | 15% | | |
| Validation Robustness | /10 | 10% | | |
| Error Handling | /10 | 5% | | |
| Logging & Observability | /10 | 5% | | |
| Scalability Potential | /10 | 5% | | |
| **OVERALL SCORE** | **/100** | 100% | | |

**Scoring Guide**:
- 9-10: Exceptional, production-ready, best-in-class
- 7-8: Good, minor improvements needed
- 5-6: Adequate, significant improvements recommended
- 3-4: Below expectations, major rework needed
- 1-2: Critical issues, fundamental redesign required

---

### Task 3: Top 6-10 Improvement Recommendations

For each recommendation, provide:

```markdown
## Improvement #N: [Title]

### Current State
[Describe what the code currently does]

### Problem/Limitation
[Explain the issue and its impact]

### Recommended Solution
[Detailed technical recommendation]

### Implementation Approach
[Step-by-step implementation guidance]

### Code Example (if applicable)
[Provide sample code or pseudocode]

### Expected Benefit
[Quantify the improvement - performance, accuracy, maintainability]

### Priority
[CRITICAL / HIGH / MEDIUM / LOW]

### Effort Estimate
[Hours/Days to implement]
```

---

### Task 4: Address Specific Known Issues

#### Issue 1: Token Limit Truncation for Large XMLs

**Analyze and Recommend**:
- Review current chunking logic
- Identify why responses are being truncated
- Propose improved chunking strategy that:
  - Preserves transformation context across chunks
  - Maintains relationship mappings
  - Enables proper merging of chunked responses
  - Handles workflows with 100+ transformations

**Provide**:
- Specific code modifications
- Chunk size calculations
- Context preservation techniques
- Merge strategy for chunked outputs

#### Issue 2: Static 3-Model Generation

**Current Behavior**: Always generates stage/intermediate/marts regardless of workflow complexity

**Desired Behavior**: Intelligently determine model type based on:
- Transformation complexity
- Data flow patterns
- Source/target relationships
- Reusability potential

**Analyze and Recommend**:
- How to analyze workflow complexity
- Decision logic for model type assignment
- When to create shared staging models
- When to create intermediate transformations
- When to directly create mart models

**Provide**:
- Decision tree or flowchart for model type determination
- Code modifications to implement intelligent model assignment

#### Issue 3: Single-Workflow Processing

**Current Limitation**: Processes one XML independently, missing cross-workflow optimization opportunities

**Desired State**: Holistic processing of all 9 workflows to:
- Identify shared source tables
- Identify common transformations
- Optimize model layering across entire project
- Eliminate duplicate staging models
- Create proper DBT project structure with refs()

**Analyze and Recommend**:
- Multi-workflow analysis approach
- Dependency graph construction
- Shared model identification algorithm
- Project structure optimization strategy

**Provide**:
- Architecture for multi-workflow processing
- Code structure for cross-workflow analysis
- DBT project organization recommendations

---

### Task 5: Prompt Engineering Analysis

Specifically analyze the 466-line prompt and provide:

1. **Prompt Structure Assessment**
   - Is the prompt well-organized?
   - Are instructions clear and unambiguous?
   - Is there proper context setting?

2. **Informatica Coverage**
   - Does the prompt cover all transformation types?
   - Source Qualifiers, Expressions, Filters, Lookups, Joiners, Aggregators, Routers, Update Strategies, etc.
   - Are edge cases addressed?

3. **DBT Output Specifications**
   - Are output format requirements clear?
   - Is SQL style guide included?
   - Are naming conventions specified?
   - Is ref() usage properly instructed?

4. **Prompt Optimization Recommendations**
   - Sections to add
   - Sections to remove or consolidate
   - Clarity improvements
   - Example additions

5. **Multi-Shot vs Zero-Shot Analysis**
   - Should examples be included?
   - What examples would improve output quality?

---

### Task 6: Future Architecture Recommendation

Propose an improved architecture that addresses all known limitations:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PROPOSED: MULTI-WORKFLOW MIGRATION ARCHITECTURE           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Phase 1: Discovery & Analysis                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  Read ALL 9 XMLs → Build Global Dependency Graph → Identify Shared  │  │
│   │  Sources/Transformations → Determine Optimal DBT Project Structure  │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│   Phase 2: Intelligent Model Planning                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  Assign Model Types (staging/intermediate/marts) Based on Analysis  │  │
│   │  Plan Model Dependencies → Generate DBT DAG Structure               │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│   Phase 3: Code Generation                                                  │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │  Generate Shared Staging Models First → Generate Intermediate →     │  │
│   │  Generate Marts → Proper ref() Linking → Validation                 │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## OUTPUT FORMAT

Structure your analysis as follows:

### Document 1: Executive Summary (1-2 pages)
- Overall assessment
- Key strengths
- Critical gaps
- Top 3 priority recommendations
- Roadmap for improvements

### Document 2: Detailed Analysis Report (5-10 pages)
- Section-by-section code analysis
- Scoring matrix with justifications
- All 6-10 improvement recommendations in detail

### Document 3: Technical Implementation Guide (3-5 pages)
- Specific code changes for top priorities
- Architecture diagrams
- Implementation roadmap with effort estimates

### Document 4: Prompt Engineering Recommendations (2-3 pages)
- Current prompt assessment
- Recommended prompt modifications
- New prompt sections to add
- Example-based improvements

---

## ANALYSIS CONSTRAINTS

1. **Be Specific**: Provide exact line numbers, function names, and code snippets when referencing issues
2. **Be Actionable**: Every recommendation must include implementation guidance
3. **Be Realistic**: Consider the ~45-second per workflow performance as a baseline to maintain
4. **Be Comprehensive**: Cover all aspects of the migration pipeline
5. **Prioritize**: Clearly rank recommendations by impact and effort
6. **No Hallucinations**: Only recommend techniques that are proven and implementable

---

## SPECIFIC QUESTIONS TO ANSWER

1. Is the current XML parsing capturing all necessary Informatica metadata for accurate DBT generation?

2. What specific changes to the chunking logic would prevent response truncation while maintaining context?

3. How should the code determine whether a workflow should generate 1, 2, or 3 DBT models?

4. What data structures are needed to analyze all 9 workflows holistically before generating any DBT code?

5. Are there any Informatica transformation types not being handled correctly?

6. Is the generated DBT SQL idiomatic and following best practices?

7. How can the validation logic be strengthened to catch more issues before code is written to Snowflake?

8. What logging improvements would make debugging failed conversions easier?

9. Is the prompt engineering optimal, or are there techniques (chain-of-thought, few-shot examples) that would improve output quality?

10. What would a production-ready version of this tool look like in terms of architecture, error handling, and scalability?

---

Begin your analysis by first reading through the entire notebook, then systematically address each task outlined above. Provide your findings in a clear, structured format that can be used to prioritize and implement improvements.
```

---

## USAGE INSTRUCTIONS

### How to Use This Prompt

1. **Open your Cortex CLI**
2. **Attach your notebook file**: `BABU_INFORMATICA_TO_DBT_V2.ipynb`
3. **Copy and paste the prompt** above (everything between the ``` markers)
4. **Submit and wait for analysis**
5. **Review the structured output** covering all analysis areas

### Follow-Up Prompts

After receiving the initial analysis, use these follow-up prompts for deeper detail:

**For Chunking Strategy Deep Dive:**
```
Based on your analysis, provide detailed pseudocode for an improved chunking strategy that:
1. Calculates optimal chunk sizes based on transformation complexity
2. Preserves transformation relationships across chunks
3. Includes a merging algorithm for combining chunked responses
4. Handles workflows with 100+ transformations without truncation
Include specific token calculations and context preservation techniques.
```

**For Multi-Workflow Architecture:**
```
Design a complete multi-workflow analysis module that:
1. Reads all 9 Informatica XMLs
2. Builds a global dependency graph
3. Identifies shared sources and transformations
4. Outputs an optimal DBT project structure
Provide Python code for the dependency graph construction and analysis algorithms.
```

**For Intelligent Model Assignment:**
```
Create a decision engine that determines DBT model type (staging/intermediate/marts) based on:
1. Transformation complexity score
2. Data flow patterns
3. Source/target relationships
4. Reusability across workflows
Provide the decision tree logic and scoring algorithm with code examples.
```

**For Prompt Optimization:**
```
Rewrite the 466-line prompt with the following improvements:
1. Add chain-of-thought reasoning instructions
2. Include 2-3 few-shot examples for complex transformations
3. Improve error handling instructions
4. Add explicit output format specifications
Provide the complete optimized prompt.
```

**For Validation Enhancement:**
```
Design an enhanced validation framework that checks:
1. SQL syntax validity
2. DBT model compilation
3. Transformation logic accuracy (comparing to Informatica source)
4. ref() dependency correctness
5. Schema compatibility
Provide validation code and test cases.
```

---

## EXPECTED OUTPUT STRUCTURE

The analysis should produce approximately:

| Document | Length | Content |
|----------|--------|---------|
| Executive Summary | 1-2 pages | High-level findings, scores, priority actions |
| Detailed Analysis | 5-10 pages | Section-by-section review, all recommendations |
| Technical Guide | 3-5 pages | Code changes, architecture, implementation roadmap |
| Prompt Engineering | 2-3 pages | Prompt assessment and optimization recommendations |

**Total**: ~15-20 pages of comprehensive analysis and actionable recommendations
