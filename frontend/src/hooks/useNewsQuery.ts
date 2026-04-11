import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { getNewsFeed } from "@/repositories/newsRepository";

export function useNewsQuery(refreshNonce = 0) {
  return useQuery({
    queryKey: ["news-feed", refreshNonce],
    queryFn: () => getNewsFeed({ refresh: refreshNonce > 0 }),
    placeholderData: keepPreviousData,
  });
}
