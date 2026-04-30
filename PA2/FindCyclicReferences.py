from MapReduce import MapReduce

class FindCyclicReferences(MapReduce):
    def Map(self, parts):
        citations = {}
       
        for part in parts:
            paperA, paperB = part
            if paperA not in citations:
                citations[paperA] = [paperB]
            else:
                citations[paperA].append(paperB)
                
        return citations

    def Reduce(self, kvs):
        all_citations = {}
        
        for partials in kvs:
            for paperA, paperB in partials.items():
                if paperA in all_citations:
                    all_citations[paperA].extend(paperB)
                else:
                    all_citations[paperA] = paperB
        
        final_cyclic = {}
        
        for paperA, Bs in all_citations.items():
            for paperB in Bs:
                if paperB in all_citations:
                    if paperA in all_citations[paperB]:
                        key = str(tuple(sorted((paperA, paperB))))
                        final_cyclic[key] = 1
    
        return final_cyclic