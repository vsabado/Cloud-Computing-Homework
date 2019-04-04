import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


class Activity implements Comparable<Activity> {
    int id;
    int start;
    int finish;
    int value;

    Activity(int id, int start, int finish, int value) {
        this.id = id;
        this.start = start;
        this.finish = finish;
        this.value = value;
    }

    public void printValues() {
        System.out.println("Id: " + this.id + "; Start: " + this.start + "; Finish: " + this.finish + "; Value: " + this.value);
    }

    public int getId() {
        return id;
    }

    public int getStart() {
        return start;
    }

    public int getFinish() {
        return finish;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int compareTo(Activity activity) {
        return getStart() - activity.getStart();
    }
}


public class Homework7 {

    public static boolean doesCollide(Activity i, Activity j) {
        ArrayList<Activity> sort = new ArrayList<>();
        sort.add(i);
        sort.add(j);
        Collections.sort(sort);
        if (sort.get(1).getStart() < sort.get(0).getFinish()) {
            return true;
        } else
            return false;
    }

    public static Activity returnHigher(Activity a, Activity b) {
        if (a.getValue() > b.getValue()) {
            return a;
        } else
            return b;

    }

    public static ArrayList<Activity> listOfCombinations(Activity obj, ArrayList<Activity> a, int maxFinish) {
        ArrayList<Activity> results = new ArrayList<>();
        results.add(obj);
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i).getFinish() > maxFinish) {
//                System.out.println("Over the maxFinish: ");
//                a.get(i).printValues();
            } else {
                if (obj.getId() == a.get(i).getId()) {

                } else if (doesCollide(obj, a.get(i))) {

                } else
                    results.add(a.get(i));
            }
        }
        if (results.size() > 2) {
            results = refineList(results);
        }
        return results;
    }

    public static ArrayList<Activity> refineList(ArrayList<Activity> a) {
        ArrayList<Activity> result = new ArrayList<>();
        result.add(a.get(0));
        for (int i = 1; i < a.size(); i++) {
            for (int j = 2; j < a.size(); j++) {
                if (i == j) {
                } else if (doesCollide(a.get(i), a.get(j))) {
                    Activity temp = returnHigher(a.get(i), a.get(j));
                    result.add(temp);
                } else
                    result.add(a.get(j));
            }
        }
        return result;
    }


    public static int sumVal(ArrayList<Activity> a) {
        int sum = 0;
        for (int i = 0; i < a.size(); i++) {
            sum += a.get(i).getValue();
        }
        return sum;
    }


    public static void writeOutput(ArrayList<Activity> a, int bestVal, ArrayList<Integer> solutions) throws FileNotFoundException {
        PrintStream ps = null;
        try {
            ps = new PrintStream("output.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert ps != null;

        System.out.println("=========================================================================================================================================");
        ps.println(bestVal);
        System.out.println("Best Value: " + bestVal);
        System.out.println("Best combination: ");
        for (int i = 0; i < a.size(); i++) {
            a.get(i).printValues();
            ps.print(a.get(i).getId() + " ");
        }

        int count = 0;
        for (int i: solutions) {
            if(i == Collections.max(solutions)){
                count++;
            }
        }

        ps.println();
        if(count == 1){
            System.out.println("IT HAS A UNIQUE SOLUTION");
            ps.println("IT HAS A UNIQUE SOLUTION");
        }
        else {
            System.out.println("IT HAS MULTIPLE SOLUTIONS");
            ps.println("IT HAS MULTIPLE SOLUTIONS");
        }

        System.out.println("Created and stored result into output.txt");
    }


    public static void main(String[] args) throws IOException {
        List<String> message = Files.readAllLines(Paths.get(args[0]));
        ArrayList<Activity> activities = new ArrayList<>();
        ArrayList<Activity> combination = new ArrayList<>();
        ArrayList<Activity> highestValComb = new ArrayList<>();
        ArrayList<Integer> solutions = new ArrayList<>();


        String token[] = message.get(0).split(" ");
        int size = Integer.parseInt(token[0]);
        int maxFinish = Integer.parseInt(token[1]);

        for (int i = 1; i <= size; i++) {
            String tempToken[] = message.get(i).split(" ");
            Activity temp = new Activity(Integer.parseInt(tempToken[0]), Integer.parseInt(tempToken[1]), Integer.parseInt(tempToken[2]), Integer.parseInt(tempToken[3]));
            //temp.printValues();
            activities.add(temp);
        }

        //Sort list in ascending order based on start field
        Collections.sort(activities);
//        for (Activity v : activities) {
//            v.printValues();
//        }

        int maxSum = Integer.MIN_VALUE;
        for (int i = 0; i < activities.size(); i++) {
            if (activities.get(i).getFinish() > maxFinish) {
//                System.out.println("Over the maxFinish: ");
//                activities.get(i).printValues();
            } else {
                combination = listOfCombinations(activities.get(i), activities, maxFinish);
                if (sumVal(combination) >= maxSum) {
                    maxSum = sumVal(combination);
                    highestValComb = combination;
                    solutions.add(maxSum);
                }
                System.out.println("Combination " + Integer.toString(i) + ": ");
                for (Activity v : combination) {
                    v.printValues();
                }
            }

        }
        Collections.sort(highestValComb);
        writeOutput(highestValComb, maxSum, solutions);
    }
}

