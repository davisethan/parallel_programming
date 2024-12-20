import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Data implements Serializable {
    List<Integer> neighbors;
    List<Integer> previous;
    List<Integer> previousPrevious;

    public Data() {
        neighbors = new ArrayList<>();
        previous = new ArrayList<>();
        previousPrevious = new ArrayList<>();
    }

    public Data(List<Integer> neighbors, List<Integer> previous, List<Integer> previousPrevious) {
        this.neighbors = new ArrayList<>(neighbors);
        this.previous = new ArrayList<>(previous);
        this.previousPrevious = new ArrayList<>(previousPrevious);
    }
}