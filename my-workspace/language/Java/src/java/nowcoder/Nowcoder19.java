package java.nowcoder;
import java.util.ArrayList;
	// 输入一个矩阵，按照从外向里以顺时针的顺序依次打印出每一个数字，
	// 例如，如果输入如下4 X 4矩阵： 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 
	// 则依次打印出数字1,2,3,4,8,12,16,15,14,13,9,5,6,7,11,10.
public class Nowcoder19{
	// 方法1:事先定义遍历的方向,并按照优先级放置——右下左上
	// 每次遍历时一次性遍历完某个方向直到下一步出界,然后再更换下一个方向
	// 直到所有方向都顺序遍历了一次,然后继续下一次循环
	public ArrayList<Integer> printMatrix(int [][] matrix) {
		// 创建返回列表
		ArrayList<Integer> printList = new ArrayList<>();
		// 排除特殊情况
		if(matrix == null || matrix.length == 0 || matrix[0].length == 0)
			return printList;
		// 定义访问标记数组
		int row = matrix.length, col = matrix[0].length;
		boolean[][] isVisited = new boolean[row][col];
		// 定义方向数组
		int[][] directions = new int[][]{
			{0,1},{1,0},{0,-1},{-1,0}
		};
		// 定义起点,以及终止条件
		int r = 0, c = 0;
		int newR, newC;
		int count = 0, sum = row*col;
		// 将起点也加入到返回列表中
		printList.add(matrix[r][c]);
		isVisited[r][c] = true;
		// 进行正式遍历
		while(count++ < sum){
			for(int[] direction:directions){
				newR = r + direction[0];
				newC = c + direction[1];
				// 当下一个节点未越界且未访问则当前方向一次性访问到底
				while(newR>=0 && newR<row && newC>=0 && newC<col && !isVisited[newR][newC]){
					r = newR;
					c = newC;
					printList.add(matrix[r][c]);
					isVisited[r][c] = true;
					newR += direction[0];
					newC += direction[1];
				}
			}
		}
		return printList;
	}
}