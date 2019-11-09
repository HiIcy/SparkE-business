package dao
import domain.Top10Category
trait ITop10CategoryDAO {
    def insert(top10Category: Top10Category)
}
