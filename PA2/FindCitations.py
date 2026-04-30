from MapReduce import MapReduce

class FindCitations(MapReduce):

    def Map(self, parts):
        #calculating the number of citations for each paper
        num_citation = {}

        for _, incoming_edge in parts:
            num_citation[incoming_edge] = num_citation.get(incoming_edge, 0) + 1
        
        print("running FindCitations Map BİTTİ")
        return num_citation

    def Reduce(self, kvs):
        #aggregate citation counts
        cumulative_num_citation = {}
        
        for partial_count in kvs:
            for paper_id, citation_count in partial_count.items():
                cumulative_num_citation[paper_id] = cumulative_num_citation.get(paper_id, 0) + citation_count

        return cumulative_num_citation