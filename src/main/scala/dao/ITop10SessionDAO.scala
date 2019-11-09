package dao

import domain.Top10Session

trait ITop10SessionDAO {
    def insert(top10Session:Top10Session):Unit
}
