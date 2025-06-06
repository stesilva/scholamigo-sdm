// Find a user by email who does NOT already have the target scholarship
MATCH (u1:Person {email: $targetEmail})
WHERE NOT (u1)-[:HAS_SCHOLARSHIP]->()

WITH u1

// Find other users who have the target scholarship
MATCH (u2:Person)
WHERE u2 <> u1 AND (u2)-[:HAS_SCHOLARSHIP]->(:Scholarship {scholarship_name: $targetScholarship})

// Calculate similarity
WITH u1, u2,

     // Count of shared skills
     SIZE([(u1)-[:HAS_SKILL]->(skill)<-[:HAS_SKILL]-(u2) | skill]) AS skillsMatch,
     
     // Count of shared languages
     SIZE([(u1)-[:SPEAKS]->(lang)<-[:SPEAKS]-(u2) | lang]) AS languagesMatch,
     
     // Count of shared degrees/education
     SIZE([(u1)-[:HAS_DEGREE]->(degree)<-[:HAS_DEGREE]-(u2) | degree]) AS educationMatch,
     
     // Count of shared certifications
     SIZE([(u1)-[:HAS_CERTIFICATION]->(cert)<-[:HAS_CERTIFICATION]-(u2) | cert]) AS certsMatch,
     
     // Count of shared honors
     SIZE([(u1)-[:HAS_HONOR]->(honor)<-[:HAS_HONOR]-(u2) | honor]) AS honorsMatch,
     
     // Count of shared work experiences
     SIZE([(u1)-[:WORKED_AT]->(company)<-[:WORKED_AT]-(u2) | company]) AS companiesMatch

// Compute total similarity score as sum of all matches
WITH u2, skillsMatch + languagesMatch + educationMatch + certsMatch + honorsMatch + companiesMatch AS similarityScore

// Get top 5 most similar users
ORDER BY similarityScore DESC
LIMIT 5

// Return their emails and similarity scores
RETURN u2.email AS SimilarUser, similarityScore AS TotalMatches
