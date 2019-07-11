package com.spark.files.csv;

import java.io.Serializable;

public class Movie implements Serializable {
    private int rank;
    private String title, genres;

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public Movie() {
    }

    public Movie(int rank, String title, String genres) {
        this.rank = rank;
        this.title = title;
        this.genres = genres;
    }

    public static Movie parseMovie(String line)
    {
        String[] mInfo = line.split(",");
        return new Movie(Integer.parseInt(mInfo[0]), mInfo[1], mInfo[2]);
    }

    @Override
    public String toString() {
        return String.join(",", String.valueOf(rank), title, genres);
    }
}
