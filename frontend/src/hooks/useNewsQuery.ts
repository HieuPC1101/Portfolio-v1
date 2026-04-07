import { useQuery } from "@tanstack/react-query";
import { getNewsFeed } from "@/repositories/newsRepository";

export function useNewsQuery() {
  return useQuery({
    queryKey: ["news-feed"],
    queryFn: getNewsFeed,
  });
}
